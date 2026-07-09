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

// Regression tests for a goroutine leak that used to occur whenever a
// StreamInterface obtained from AsyncSubresourceHelper (VSOCK, PortForward,
// VNC, USBRedir, SerialConsole, ...) was consumed through AsConn() instead of
// Stream(). See streamer.go's wsConn.Close() / wsStreamer.streamDone() for
// the fix: closing the net.Conn returned by AsConn() now also releases the
// AsyncWSRoundTripper goroutine, the same way returning from Stream() always
// did.
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

// TestVSOCKAsConnDoesNotLeakGoroutine dials through the real
// AsyncSubresourceHelper, consumes the result via AsConn() (the only sane
// way to use e.g. VSOCK or PortForward, since they carry an arbitrary
// protocol rather than a single in/out byte stream), and closes the
// connection. The goroutine count must return to baseline, and no goroutine
// should remain parked in AsyncWSRoundTripper.WebsocketCallback.
func TestVSOCKAsConnDoesNotLeakGoroutine(t *testing.T) {
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

	t.Logf("goroutines: before=%d after=%d", before, after)
	if after > before {
		t.Fatalf("expected no leaked goroutines after closing the AsConn() connection, but count grew: before=%d after=%d\n%s", before, after, goroutineDump(t))
	}

	if dump := goroutineDump(t); strings.Contains(dump, "WebsocketCallback") {
		t.Fatalf("did not expect any goroutine parked in AsyncWSRoundTripper.WebsocketCallback, got profile:\n%s", dump)
	}
}

// TestVSOCKStreamDoesNotLeak is a control case: consuming the same helper via
// Stream() instead of AsConn() has never leaked, since Stream() has always
// called streamDone() on return. This guards against a regression in that
// path while fixing the AsConn() path.
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

	t.Logf("goroutines: before=%d after=%d", before, after)
	if after > before {
		t.Fatalf("did not expect a leak when using Stream(): before=%d after=%d\n%s", before, after, goroutineDump(t))
	}
}

// TestVSOCKAsConnGoroutineDoesNotAccumulate repeats the AsConn() connect and
// close cycle many times. Before the fix this leaked exactly one goroutine
// per iteration; now the count must stay flat.
func TestVSOCKAsConnGoroutineDoesNotAccumulate(t *testing.T) {
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

	t.Logf("goroutines: before=%d after=%d over %d iterations", before, after, iterations)
	if after > before {
		t.Fatalf("expected no accumulation over %d iterations, got before=%d after=%d\n%s", iterations, before, after, goroutineDump(t))
	}
}

// TestVSOCKAsConnCloseIsIdempotent guards the sync.Once in
// wsStreamer.streamDone(): closing the net.Conn returned by AsConn() more
// than once must not panic, even though both calls race to close the same
// `done` channel.
func TestVSOCKAsConnCloseIsIdempotent(t *testing.T) {
	server := newEchoWebsocketServer(t)
	defer server.Close()

	config := &rest.Config{Host: server.URL}

	stream, err := kvcorev1.AsyncSubresourceHelper(config, "virtualmachineinstances", "default", "testvmi", "vsock", nil)
	if err != nil {
		t.Fatalf("AsyncSubresourceHelper failed: %v", err)
	}

	conn := stream.AsConn()
	if err := conn.Close(); err != nil {
		t.Fatalf("first Close() failed: %v", err)
	}
	// A second Close() may return an error (the underlying connection is
	// already closed), but it must not panic.
	_ = conn.Close()
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
// same mechanism used by pprof's memory/goroutine profiler, so a leak (if
// any) can be pinpointed to an exact function instead of just a changed
// count.
func goroutineDump(t *testing.T) string {
	t.Helper()
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 2); err != nil {
		t.Fatalf("failed to capture goroutine profile: %v", err)
	}
	return buf.String()
}
