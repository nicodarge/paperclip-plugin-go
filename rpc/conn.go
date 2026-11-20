package rpc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
)

// MaxLineBytes bounds one line. The host drops a message above 128 MiB of
// UTF-16 code units (MAX_WORKER_MESSAGE_CHARS), which UTF-8 encodes in at most
// three bytes each, so a longer line could never have been delivered.
const MaxLineBytes = 3 * 128 * 1024 * 1024

// ErrLineTooLong reports a line dropped for exceeding MaxLineBytes.
var ErrLineTooLong = errors.New("line exceeds the protocol's size limit")

// Handlers receives what a Conn reads, on the reading goroutine and in arrival
// order. A handler that blocks stalls the stream; hand long work to a goroutine.
// A handler that calls Call, or a Worker's CallHost, and waits deadlocks: the
// response it waits for can only be read by the goroutine it is blocking.
// Responses never reach it: they settle the request they answer.
type Handlers struct {
	Request      func(Envelope)
	Notification func(Envelope)
	// Other receives a JSON-RPC 2.0 object that is none of the three kinds.
	Other func(Envelope)
	// Unparseable receives a line that is not a JSON-RPC 2.0 object, or a nil
	// line with ErrLineTooLong.
	Unparseable func(line []byte, err error)
}

// Conn is one NDJSON JSON-RPC stream with its own outbound id space.
type Conn struct {
	// KeepPendingOnClose, set before Serve and left alone afterwards, changes
	// what the end of the stream, or a failure to read it, does to outgoing
	// requests: Serve then returns
	// without settling the ones still pending, which stay pending, and Request
	// keeps working as before the end (it writes the message, and answers with
	// WORKER_UNAVAILABLE only when the write fails). It is for a Conn whose
	// peer's exit is reported to the host by other means, such as a router that
	// ends its own process so that the host rejects the forwarded calls itself.
	// The default settles every pending request and refuses later ones.
	KeepPendingOnClose bool

	r       *bufio.Reader
	maxLine int

	wmu sync.Mutex
	w   io.Writer

	mu      sync.Mutex
	next    int64
	pending map[int64]func(Envelope)
	closed  bool
}

// NewConn wraps a reader and a writer. Each message is written with a single
// Write call.
func NewConn(r io.Reader, w io.Writer) *Conn {
	return &Conn{r: bufio.NewReaderSize(r, 64*1024), maxLine: MaxLineBytes, w: w, pending: map[int64]func(Envelope){}}
}

// Send writes one message.
func (c *Conn) Send(e Envelope) error { return c.write(e.Encode()) }

// WriteRaw writes one line untouched.
func (c *Conn) WriteRaw(line []byte) error {
	if n := len(line); n == 0 || line[n-1] != '\n' {
		line = append(append(make([]byte, 0, n+1), line...), '\n')
	}
	return c.write(line)
}

func (c *Conn) write(b []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.w.Write(b)
	return err
}

// Request sends e under a fresh id and calls reply at most once: with the
// peer's response, or with an error response made up here when the message
// cannot be written or the stream has ended (unless KeepPendingOnClose is set).
// The response still carries this Conn's id; the caller re-labels it. A peer
// that never answers leaves the entry pending until the stream ends (unless
// KeepPendingOnClose is set). It returns a function that drops the
// entry, after which a late response is ignored.
//
// reply runs on the reading goroutine when the peer's response or the end of
// the stream settles the request, so it must not block and must not call Call
// or a Worker's CallHost: they wait for a response only the reading goroutine
// can deliver.
func (c *Conn) Request(e Envelope, reply func(Envelope)) (forget func()) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		reply(connectionClosed())
		return func() {}
	}
	c.next++
	id := c.next
	c.pending[id] = reply
	c.mu.Unlock()

	if err := c.Send(e.With("id", strconv.AppendInt(nil, id, 10))); err != nil {
		if c.take(id) != nil {
			reply(NewError(nil, Errorf(CodeWorkerUnavailable, "cannot send %q: %v", e.Method(), err)))
		}
	}
	return func() { c.take(id) }
}

func (c *Conn) take(id int64) func(Envelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply := c.pending[id]
	delete(c.pending, id)
	return reply
}

// Call is Request for a caller that waits. The error is ctx's when it ends first.
func (c *Conn) Call(ctx context.Context, e Envelope) (Envelope, error) {
	done := make(chan Envelope, 1)
	forget := c.Request(e, func(resp Envelope) { done <- resp })
	select {
	case resp := <-done:
		return resp, nil
	case <-ctx.Done():
		forget()
		return nil, ctx.Err()
	}
}

// Pending reports how many requests await a response.
func (c *Conn) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

func connectionClosed() Envelope {
	return NewError(nil, Errorf(CodeWorkerUnavailable, "connection closed"))
}

// Serve reads messages until the stream ends, which it reports as nil, or fails
// to read, which it reports as that error, and then answers every request still pending, and any made afterwards, with a
// WORKER_UNAVAILABLE error, unless KeepPendingOnClose is set. Call it once: two
// concurrent Serve calls would interleave reads of the same stream.
func (c *Conn) Serve(h Handlers) error {
	err := c.read(h)
	if c.KeepPendingOnClose {
		return err
	}
	c.mu.Lock()
	c.closed = true
	ids := make([]int64, 0, len(c.pending))
	for id := range c.pending {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	for _, id := range ids {
		if reply := c.take(id); reply != nil {
			reply(connectionClosed())
		}
	}
	return err
}

func (c *Conn) read(h Handlers) error {
	for {
		line, err := c.readLine()
		if errors.Is(err, ErrLineTooLong) {
			if h.Unparseable != nil {
				h.Unparseable(nil, err)
			}
			continue
		}
		if len(trimSpace(line)) > 0 {
			c.dispatch(line, h)
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (c *Conn) dispatch(line []byte, h Handlers) {
	e, err := Parse(line)
	if err != nil {
		if h.Unparseable != nil {
			h.Unparseable(line, err)
		}
		return
	}
	switch e.Kind() {
	case KindResponse:
		id, err := strconv.ParseInt(string(e.ID()), 10, 64)
		if err != nil {
			return
		}
		if reply := c.take(id); reply != nil {
			reply(e)
		}
	case KindRequest:
		if h.Request != nil {
			h.Request(e)
		}
	case KindNotification:
		if h.Notification != nil {
			h.Notification(e)
		}
	default:
		if h.Other != nil {
			h.Other(e)
		}
	}
}

// readLine returns one line including its terminator (absent on a last line cut
// by EOF). A line over MaxLineBytes is consumed and reported as ErrLineTooLong.
func (c *Conn) readLine() ([]byte, error) {
	var line []byte
	tooLong := false
	for {
		chunk, err := c.r.ReadSlice('\n')
		if !tooLong {
			if len(line)+len(chunk) > c.maxLine {
				tooLong, line = true, nil
			} else {
				line = append(line, chunk...)
			}
		}
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case tooLong && (err == nil || errors.Is(err, io.EOF)):
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, ErrLineTooLong
		default:
			return line, err
		}
	}
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r' || b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return b
}
