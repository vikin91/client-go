/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors
 *
 */

// This file is a standalone bug reproducer for a goroutine leak in
// AsyncSubresourceHelper / wsStreamer, filed upstream. It intentionally
// avoids ginkgo so it can be copy-pasted into an issue/PR as a plain `go test`.
package v1_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"

	kvcorev1 "kubevirt.io/client-go/kubevirt/typed/core/v1"
)

// TestVSOCKAsConnGoroutineLeak reproduces a goroutine leak that occurs every
// time a StreamInterface obtained from AsyncSubresourceHelper (e.g. via
// VirtualMachineInstanceInterface.VSOCK()) is consumed through AsConn()
// instead of Stream().
//
// Root cause: AsyncSubresourceHelper spawns a goroutine that performs the
// websocket RoundTrip and then blocks inside AsyncWSRoundTripper.WebsocketCallback
// on `<-aws.Done`. The `done` channel is only ever closed by
// wsStreamer.streamDone(), which is only called from wsStreamer.Stream().
// Callers that use AsConn() (the only sane way to use VSOCK, since VSOCK
// carries an arbitrary application protocol, not a single in/out byte
// stream) never call Stream(), so `done` is never closed. The background
// goroutine - and everything it holds onto (the *http.Response, the
// underlying websocket connection via the RoundTrip's deferred Close, TLS
// state, etc.) - leaks for the remaining lifetime of the process, even after
// the caller closes the net.Conn returned by AsConn().
func TestVSOCKAsConnGoroutineLeak(t *testing.T) {
	server := newEchoWebsocketServer(t)
	defer server.Close()

	config := &rest.Config{Host: server.URL}

	quiesce()
	before := runtime.NumGoroutine()

	stream, err := kvcorev1.AsyncSubresourceHelper(config, "virtualmachineinstances", "default", "testvmi", "vsock", nil)
	if err != nil {
		t.Fatalf("AsyncSubresourceHelper failed: %v", err)
	}

	conn := stream.AsConn()
	if err := conn.Close(); err != nil {
		t.Fatalf("failed to close the connection returned by AsConn(): %v", err)
	}

	quiesce()
	after := runtime.NumGoroutine()

	t.Logf("goroutines: before=%d after=%d (closing the AsConn() connection should not leave any behind)", before, after)
	if after <= before {
		t.Fatalf("expected the goroutine count to stay elevated after closing the AsConn() connection (leak), but it didn't: before=%d after=%d", before, after)
	}

	dump := goroutineDump(t)
	if !strings.Contains(dump, "WebsocketCallback") {
		t.Fatalf("expected a leaked goroutine parked in AsyncWSRoundTripper.WebsocketCallback, got profile:\n%s", dump)
	}
	t.Logf("confirmed leaked goroutine, stuck forever in WebsocketCallback:\n%s", relevantStack(dump, "WebsocketCallback"))
}

// TestVSOCKStreamDoesNotLeak is a control case: when the very same helper is
// consumed via Stream() instead of AsConn(), the background goroutine
// terminates correctly once Stream() returns, because Stream() calls
// streamDone() which closes `done`. This isolates the bug to the AsConn()
// path.
func TestVSOCKStreamDoesNotLeak(t *testing.T) {
	server := newEchoWebsocketServer(t)
	defer server.Close()

	config := &rest.Config{Host: server.URL}

	quiesce()
	before := runtime.NumGoroutine()

	stream, err := kvcorev1.AsyncSubresourceHelper(config, "virtualmachineinstances", "default", "testvmi", "vsock", nil)
	if err != nil {
		t.Fatalf("AsyncSubresourceHelper failed: %v", err)
	}

	in := strings.NewReader("bye")
	var out bytes.Buffer
	_ = stream.Stream(kvcorev1.StreamOptions{In: in, Out: &out})

	quiesce()
	after := runtime.NumGoroutine()

	t.Logf("goroutines: before=%d after=%d (Stream() closes `done`, so this should not leak)", before, after)
	if after > before {
		t.Fatalf("did not expect a leak when using Stream(): before=%d after=%d\n%s", before, after, goroutineDump(t))
	}
}

// TestVSOCKAsConnGoroutineLeakAccumulates shows that the leak is unbounded:
// every VSOCK-style connect+close cycle through AsConn() leaves one more
// goroutine parked forever, so a long-running process (e.g. a controller or
// CLI that repeatedly opens VSOCK connections) will eventually exhaust
// goroutines/memory.
func TestVSOCKAsConnGoroutineLeakAccumulates(t *testing.T) {
	server := newEchoWebsocketServer(t)
	defer server.Close()

	config := &rest.Config{Host: server.URL}
	const iterations = 20

	quiesce()
	before := runtime.NumGoroutine()

	for i := 0; i < iterations; i++ {
		stream, err := kvcorev1.AsyncSubresourceHelper(config, "virtualmachineinstances", "default", "testvmi", "vsock", nil)
		if err != nil {
			t.Fatalf("iteration %d: AsyncSubresourceHelper failed: %v", i, err)
		}
		conn := stream.AsConn()
		if err := conn.Close(); err != nil {
			t.Fatalf("iteration %d: failed to close conn: %v", i, err)
		}
	}

	quiesce()
	after := runtime.NumGoroutine()
	leaked := after - before

	t.Logf("goroutines: before=%d after=%d leaked=%d over %d iterations", before, after, leaked, iterations)
	if leaked < iterations {
		t.Fatalf("expected roughly %d leaked goroutines (one per iteration), got only %d (before=%d after=%d)", iterations, leaked, before, after)
	}
}

// newEchoWebsocketServer starts an httptest server that upgrades every
// request to a websocket connection and holds it open, mirroring how
// virt-handler holds a vsock proxy connection open until the client hangs up.
func newEchoWebsocketServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := kvcorev1.NewUpgrader()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
}

// quiesce gives background goroutines a chance to reach a steady state
// before we sample runtime.NumGoroutine(), to keep the counts comparable.
func quiesce() {
	for i := 0; i < 3; i++ {
		runtime.Gosched()
	}
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
}

// goroutineDump captures a full goroutine profile (stacks included), the
// same mechanism used by pprof's memory/goroutine profiler, so the leak can
// be pinpointed to an exact function instead of just a changed count.
func goroutineDump(t *testing.T) string {
	t.Helper()
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 2); err != nil {
		t.Fatalf("failed to capture goroutine profile: %v", err)
	}
	return buf.String()
}

// relevantStack extracts just the goroutine stack(s) containing needle, to
// keep failure output readable.
func relevantStack(dump, needle string) string {
	blocks := strings.Split(dump, "\n\n")
	var matches []string
	for _, b := range blocks {
		if strings.Contains(b, needle) {
			matches = append(matches, b)
		}
	}
	return strings.Join(matches, "\n\n")
}
