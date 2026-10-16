package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"
)

func verifyPeerReachability(ctx context.Context, runner commandRunner, hostIP string) error {
	address, err := netip.ParseAddr(hostIP)
	if err != nil || !address.Is4() || address.IsUnspecified() || address.IsMulticast() {
		return errors.New("proxy reachability requires an explicit host IPv4 address")
	}
	err = verifyLoopbackReachability(ctx, func(probe context.Context, port int, challenge string) error {
		url := fmt.Sprintf("http://%s:%d/%s", hostIP, port, challenge)
		_, err := runner.peer(probe, "wget -Y off -T 3 -qO- "+quote(url))
		return err
	})
	if err != nil {
		return fmt.Errorf("owned network peer cannot reach a host listener bound to 127.0.0.1 through %s; proxy availability is unsupported on this host network: %w", hostIP, err)
	}
	return nil
}

func verifyLoopbackReachability(ctx context.Context, connect func(context.Context, int, string) error) error {
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := probe.Err(); err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return err
	}
	defer listener.Close()
	stopListener := context.AfterFunc(probe, func() { listener.Close() })
	defer stopListener()
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return err
	}
	challenge := hex.EncodeToString(bytes)
	received := make(chan error, 1)
	go func() {
		connection, err := listener.AcceptTCP()
		if err != nil {
			received <- err
			return
		}
		defer connection.Close()
		stopConnection := context.AfterFunc(probe, func() { connection.Close() })
		defer stopConnection()
		deadline, _ := probe.Deadline()
		if err := connection.SetDeadline(deadline); err != nil {
			received <- err
			return
		}
		request, err := http.ReadRequest(bufio.NewReader(io.LimitReader(connection, 4096)))
		if err == nil {
			defer request.Body.Close()
		}
		if err == nil && (request.Method != "GET" || request.RequestURI != "/"+challenge) {
			err = errors.New("network peer did not return the reachability challenge")
		}
		response := "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
		if err != nil {
			response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
		}
		_, writeErr := io.WriteString(connection, response)
		received <- errors.Join(err, writeErr)
	}()
	connectErr := connect(probe, listener.Addr().(*net.TCPAddr).Port, challenge)
	if connectErr != nil {
		cancel()
	}
	receiveErr := <-received
	if connectErr != nil || receiveErr != nil {
		return errors.Join(connectErr, receiveErr, probe.Err())
	}
	return probe.Err()
}
