// Package guestnet provides small network transports for trusted guest helpers.
package guestnet

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"time"
)

// Forward connects stdin/stdout to an explicit unicast IPv4 TCP destination.
// Input EOF half-closes TCP so the final reply can drain. A closable input is
// owned by this call and closed on completion or cancellation. Other readers
// must finish their own blocked reads when the caller cancels its operation.
func Forward(ctx context.Context, address string, input io.Reader, output io.Writer) error {
	target, err := netip.ParseAddrPort(address)
	if err != nil || !target.Addr().Is4() || target.Addr().IsUnspecified() || target.Addr().IsMulticast() || target.Port() == 0 {
		return errors.New("forward requires an explicit unicast IPv4 address and TCP port")
	}
	if input == nil {
		input = emptyInput{}
	}
	if output == nil {
		output = io.Discard
	}
	closeInput := func() {
		if closer, ok := input.(io.Closer); ok {
			closer.Close()
		}
	}
	defer closeInput()
	dialer := net.Dialer{Timeout: 10 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp4", target.String())
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { connection.Close(); closeInput() })
	defer stop()
	defer connection.Close()
	writing := make(chan error, 1)
	go func() {
		_, err := io.Copy(connection, input)
		if err == nil {
			err = connection.(*net.TCPConn).CloseWrite()
		} else {
			connection.Close()
		}
		writing <- err
	}()
	_, readErr := io.Copy(output, connection)
	connection.Close()
	closeInput()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	select {
	case writeErr := <-writing:
		if errors.Is(writeErr, net.ErrClosed) || errors.Is(writeErr, io.ErrClosedPipe) || errors.Is(writeErr, os.ErrClosed) {
			writeErr = nil
		}
		return errors.Join(readErr, writeErr)
	default:
		return readErr
	}
}

type emptyInput struct{}

func (emptyInput) Read([]byte) (int, error) { return 0, io.EOF }
