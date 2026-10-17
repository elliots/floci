//go:build !linux

package guestfs

import "errors"

func Writeback(_ []string, _ bool) error {
	return errors.New("persistent filesystem writeback requires Linux")
}
