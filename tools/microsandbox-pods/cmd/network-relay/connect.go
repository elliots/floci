package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/netip"
	"strconv"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guestnet"
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
	return guestnet.Forward(context.Background(), net.JoinHostPort(ip.String(), strconv.Itoa(*port)), input, output)
}
