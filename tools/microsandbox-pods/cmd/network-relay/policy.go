package main

import (
	"errors"
	"strconv"

	"github.com/floci-io/floci/tools/microsandbox-pods/policyguard"
)

func configurePolicy(args []string) error {
	if len(args) == 1 && args[0] == "node" {
		return policyguard.InstallNode()
	}
	if len(args) == 3 && args[0] == "application" {
		port, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		return policyguard.InstallApplication(args[1], port)
	}
	return errors.New("policy requires node or application <owned-cgroup-name> <transport-port>")
}
