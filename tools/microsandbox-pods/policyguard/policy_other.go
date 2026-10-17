//go:build !linux

package policyguard

import "errors"

func InstallNode() error { return errors.New("policy socket hooks require Linux") }

func InstallApplication(name string, transportPort int) error {
	if err := validateApplication(name); err != nil {
		return err
	}
	return errors.New("policy socket hooks require Linux")
}
