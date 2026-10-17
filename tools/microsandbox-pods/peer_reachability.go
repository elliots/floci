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
	_, err := peerListenerAddress(ctx, runner, hostIP)
	return err
}

func peerHostAddress(hostIP string) (string, error) {
	address, err := netip.ParseAddr(hostIP)
	if err != nil || !address.Is4() || address.IsUnspecified() || address.IsMulticast() || address == netip.MustParseAddr("255.255.255.255") {
		return "", errors.New("node-ip must be an explicit unicast host IPv4 address reachable from the network peer")
	}
	return address.String(), nil
}

func peerListenerAddress(ctx context.Context, runner commandRunner, hostIP string) (string, error) {
	hostIP, err := peerHostAddress(hostIP)
	if err != nil {
		return "", err
	}
	return selectPeerListener(hostIP, localHostAddress, func(bindIP string) error {
		return verifyBoundReachability(ctx, bindIP, func(probe context.Context, port int, challenge string) error {
			url := fmt.Sprintf("http://%s:%d/%s", hostIP, port, challenge)
			_, err := runner.peer(probe, "wget -Y off -T 3 -qO- "+quote(url))
			return err
		})
	})
}

func selectPeerListener(hostIP string, local func(string) (bool, error), verify func(string) error) (string, error) {
	loopbackErr := verify("127.0.0.1")
	if loopbackErr == nil {
		return "127.0.0.1", nil
	}
	owned, err := local(hostIP)
	if err != nil || !owned || hostIP == "127.0.0.1" {
		return "", fmt.Errorf("network peer cannot reach host loopback through %s and node-ip is not a usable local interface: %w", hostIP, errors.Join(loopbackErr, err))
	}
	if err := verify(hostIP); err != nil {
		return "", fmt.Errorf("network peer cannot reach the explicit host interface %s: %w", hostIP, errors.Join(loopbackErr, err))
	}
	return hostIP, nil
}

func localHostAddress(hostIP string) (bool, error) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return false, err
	}
	for _, address := range addresses {
		prefix, err := netip.ParsePrefix(address.String())
		if err == nil && prefix.Addr().String() == hostIP {
			return true, nil
		}
	}
	return false, nil
}

func verifyLoopbackReachability(ctx context.Context, connect func(context.Context, int, string) error) error {
	return verifyBoundReachability(ctx, "127.0.0.1", connect)
}

func verifyBoundReachability(ctx context.Context, bindIP string, connect func(context.Context, int, string) error) error {
	bindIP, err := peerHostAddress(bindIP)
	if err != nil {
		return err
	}
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := probe.Err(); err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.ParseIP(bindIP)})
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
