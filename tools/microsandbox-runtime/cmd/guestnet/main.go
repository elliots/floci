package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guestnet"
)

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
	forward := flags.String("forward", "", "connect stdin/stdout to IPv4:port")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *forward == "" {
		return errors.New("forward target is required")
	}
	return guestnet.Forward(ctx, *forward, os.Stdin, os.Stdout)
}
