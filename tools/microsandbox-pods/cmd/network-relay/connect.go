package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/netip"
	"strconv"
	"time"
)

func connectStdio(args []string, input io.Reader, output io.Writer) error {
	flags := flag.NewFlagSet("connect", flag.ContinueOnError)
	address := flags.String("address", "", "IPv4 destination")
	port := flags.Int("port", 0, "TCP destination port")
	if err := flags.Parse(args); err != nil {
		return err
	}
	ip, err := netip.ParseAddr(*address)
	if err != nil || !ip.Is4() || ip.IsUnspecified() || ip.IsMulticast() || *port < 1 || *port > 65535 || flags.NArg() != 0 {
		return errors.New("connect requires an explicit unicast IPv4 address and TCP port")
	}
	dialer := net.Dialer{Timeout: 10 * time.Second}
	connection, err := dialer.DialContext(context.Background(), "tcp4", net.JoinHostPort(ip.String(), strconv.Itoa(*port)))
	if err != nil {
		return err
	}
	defer connection.Close()
	if closer, ok := input.(io.Closer); ok {
		defer closer.Close()
	}
	// The command owns its input. Closing it when the server finishes releases a
	// blocked stdin read; an input EOF half-closes TCP while the reply drains.
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
	if closer, ok := input.(io.Closer); ok {
		closer.Close()
	}
	select {
	case writeErr := <-writing:
		if errors.Is(writeErr, net.ErrClosed) || errors.Is(writeErr, io.ErrClosedPipe) {
			writeErr = nil
		}
		return errors.Join(readErr, writeErr)
	default:
		// main exits after returning. Arbitrary embedded readers must arrange
		// cancellation themselves; production stdin is a closable file.
		return readErr
	}
}
