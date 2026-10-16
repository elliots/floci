package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guestnet"
)

type configuration struct {
	DNSRouting *guestnet.DNSRouting `json:"dnsRouting,omitempty"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "Guest network helper failed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("guestnet", flag.ContinueOnError)
	configPath := flags.String("config", "", "private JSON DNS routing configuration")
	forward := flags.String("forward", "", "connect stdin/stdout to IPv4:port")
	configureDNS := flags.Bool("configure-dns", false, "configure kernel DNS routing and exit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *forward != "" {
		if *configPath != "" || *configureDNS {
			return errors.New("forward and DNS routing configuration are mutually exclusive")
		}
		return guestnet.Forward(ctx, *forward, os.Stdin, os.Stdout)
	}
	if !*configureDNS {
		return errors.New("DNS routing requires --configure-dns")
	}
	file, err := os.Open(*configPath)
	if err != nil {
		return errors.New("cannot open guest DNS configuration")
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 65537))
	decoder.DisallowUnknownFields()
	var config configuration
	if err := decoder.Decode(&config); err != nil {
		return errors.New("invalid guest DNS configuration")
	}
	if !errors.Is(decoder.Decode(new(any)), io.EOF) {
		return errors.New("guest DNS routing needs exactly one configuration")
	}
	if config.DNSRouting == nil {
		return errors.New("DNS routing requires a nameservers configuration")
	}
	return guestnet.ConfigureDNS(*config.DNSRouting)
}
