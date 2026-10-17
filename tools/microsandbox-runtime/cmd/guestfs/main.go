package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guestfs"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "guestfs:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("expected ownership, writeback or runc")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	switch args[0] {
	case "ownership":
		group := flags.Int64("group", -1, "Filesystem group")
		readOnly := flags.Bool("read-only", false, "Apply read-only volume permission mask")
		mismatch := flags.Bool("on-root-mismatch", false, "Skip a volume whose root already matches")
		rootOnly := flags.Bool("root-only", false, "Change only the volume directory")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 1 {
			return errors.New("ownership requires exactly one volume root")
		}
		return (guestfs.Ownership{Group: *group, ReadOnly: *readOnly, OnRootMismatch: *mismatch, RootOnly: *rootOnly}).Apply(flags.Arg(0))
	case "writeback":
		seal := flags.Bool("seal", false, "Leave persistent filesystems frozen for guest removal")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		return guestfs.Writeback(flags.Args(), *seal)
	case "runc":
		groups := flags.String("groups", "", "Comma-separated supplemental groups")
		policy := flags.String("policy", "Merge", "Merge or Strict")
		binary := flags.String("binary", "runc", "Real OCI runtime executable")
		state := flags.String("state", "", "Private per-container resolved group identity")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		settings, err := guestfs.ParseGroups(*groups, *policy)
		if err != nil {
			return err
		}
		if *state == "" {
			return errors.New("runc requires a private --state file")
		}
		settings.StateFile = *state
		if err := settings.PrepareRunc(flags.Args()); err != nil {
			return err
		}
		resolved, err := exec.LookPath(*binary)
		if err != nil {
			return err
		}
		return syscall.Exec(resolved, append([]string{resolved}, flags.Args()...), os.Environ())
	default:
		return errors.New("expected ownership, writeback or runc")
	}
}
