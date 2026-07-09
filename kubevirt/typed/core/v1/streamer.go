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

package v1

import (
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type wsStreamer struct {
	conn      *websocket.Conn
	done      chan struct{}
	closeOnce sync.Once
}

// streamDone signals AsyncSubresourceHelper's round-tripper goroutine that
// this stream is finished, so it can let its deferred conn.Close() run. It
// is called both when Stream() returns and when the net.Conn returned by
// AsConn() is closed, so it must be safe to call more than once (e.g. if a
// caller closes the AsConn() connection *and* the wsStreamer was also
// obtained through NewWebsocketStreamer with an externally managed done
// channel).
func (ws *wsStreamer) streamDone() {
	ws.closeOnce.Do(func() { close(ws.done) })
}

func (ws *wsStreamer) Stream(options StreamOptions) error {
	copyErr := make(chan error, 1)

	go func() {
		_, err := CopyTo(ws.conn, options.In)
		copyErr <- err
	}()

	go func() {
		_, err := CopyFrom(options.Out, ws.conn)
		copyErr <- err
	}()

	defer ws.streamDone()
	return <-copyErr
}

func (ws *wsStreamer) AsConn() net.Conn {
	return &wsConn{
		Conn:         ws.conn,
		binaryReader: &binaryReader{conn: ws.conn},
		binaryWriter: &binaryWriter{conn: ws.conn},
		streamDone:   ws.streamDone,
	}
}

type wsConn struct {
	*websocket.Conn
	*binaryReader
	*binaryWriter
	// streamDone is wsStreamer.streamDone, carried over so that closing the
	// net.Conn returned by AsConn() also unblocks AsyncWSRoundTripper's
	// round-trip goroutine. Without this, that goroutine (and everything it
	// holds onto) leaks for the life of the process, because it otherwise
	// only unblocks when Stream() returns - which AsConn() callers never
	// call.
	streamDone func()
}

// Close closes the underlying websocket connection and releases the
// AsyncSubresourceHelper goroutine that dialed it. The round-tripper this
// connection came from also has its own deferred close of the same
// websocket.Conn once that goroutine unblocks; closing it here first is
// intentional and the resulting second Close() call is a harmless no-op.
func (c *wsConn) Close() error {
	defer c.streamDone()
	return c.Conn.Close()
}

func (c *wsConn) SetDeadline(t time.Time) error {
	if err := c.Conn.SetWriteDeadline(t); err != nil {
		return err
	}
	return c.Conn.SetReadDeadline(t)
}

func NewWebsocketStreamer(conn *websocket.Conn, done chan struct{}) *wsStreamer {
	return &wsStreamer{
		conn: conn,
		done: done,
	}
}
