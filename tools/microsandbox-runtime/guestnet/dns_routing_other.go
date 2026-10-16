//go:build !linux

package guestnet

import "errors"

func ConfigureDNS(DNSRouting) error {
	return errors.New("guest DNS routing requires Linux network administration privileges")
}
