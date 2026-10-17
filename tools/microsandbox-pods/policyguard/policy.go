// Package policyguard installs persistent, daemon-free cgroup socket hooks.
package policyguard

import (
	"errors"
	"regexp"
)

// NodeMark belongs only to trusted node processes, never application traffic.
const NodeMark = 0x40000000

var applicationCgroup = regexp.MustCompile(`^floci-application-g[0-9]+-[a-z0-9]+$`)

func validateApplication(name string) error {
	if !applicationCgroup.MatchString(name) {
		return errors.New("policy guard requires an owned application cgroup")
	}
	return nil
}
