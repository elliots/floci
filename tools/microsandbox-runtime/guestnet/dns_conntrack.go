package guestnet

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"slices"

	"github.com/mdlayher/netlink"
)

// Constants are from Linux's public nfnetlink_conntrack.h ABI. Keep the original
// tuple, zone and ID in deletion requests, so expiry cannot delete a replacement
// flow that reused the same tuple between the dump and deletion.
const (
	ctTupleOriginal = 1
	ctID            = 12
	ctZone          = 18
	ctTupleIP       = 1
	ctTupleProtocol = 2
	ctIPv4Dst       = 2
	ctProtocolNum   = 1
	ctProtocolDst   = 3
)

func dnsConntrackDelete(message netlink.Message) (netlink.Message, bool, error) {
	if len(message.Data) < 4 || message.Data[0] != 2 {
		return netlink.Message{}, false, nil
	}
	attributes, err := netlink.UnmarshalAttributes(message.Data[4:])
	if err != nil {
		return netlink.Message{}, false, err
	}
	var selected []netlink.Attribute
	matched, hasID := false, false
	for _, attribute := range attributes {
		switch attribute.Type & 0x3fff {
		case ctTupleOriginal:
			matched, err = dnsConntrackTuple(attribute.Data)
			if err != nil {
				return netlink.Message{}, false, err
			}
			selected = append(selected, attribute)
		case ctID:
			if len(attribute.Data) != 4 {
				return netlink.Message{}, false, errors.New("invalid conntrack flow ID")
			}
			hasID = true
			selected = append(selected, attribute)
		case ctZone:
			selected = append(selected, attribute)
		}
	}
	if !matched {
		return netlink.Message{}, false, nil
	}
	if !hasID {
		return netlink.Message{}, false, errors.New("DNS conntrack flow is missing its ID")
	}
	data, err := netlink.MarshalAttributes(selected)
	if err != nil {
		return netlink.Message{}, false, err
	}
	return netlink.Message{
		Header: netlink.Header{Type: 0x102, Flags: netlink.Request | netlink.Acknowledge},
		Data:   append([]byte{2, 0, 0, 0}, data...),
	}, true, nil
}

func dnsConntrackTuple(data []byte) (bool, error) {
	attributes, err := netlink.UnmarshalAttributes(data)
	if err != nil {
		return false, err
	}
	var destination netip.Addr
	var protocol byte
	var port uint16
	for _, attribute := range attributes {
		kind := attribute.Type & 0x3fff
		if kind != ctTupleIP && kind != ctTupleProtocol {
			continue
		}
		fields, err := netlink.UnmarshalAttributes(attribute.Data)
		if err != nil {
			return false, err
		}
		for _, field := range fields {
			switch {
			case kind == ctTupleIP && field.Type&0x3fff == ctIPv4Dst && len(field.Data) == 4:
				destination = netip.AddrFrom4([4]byte(field.Data))
			case kind == ctTupleProtocol && field.Type&0x3fff == ctProtocolNum && len(field.Data) == 1:
				protocol = field.Data[0]
			case kind == ctTupleProtocol && field.Type&0x3fff == ctProtocolDst && len(field.Data) == 2:
				port = binary.BigEndian.Uint16(field.Data)
			}
		}
	}
	return slices.Contains(dnsVirtualAddresses[:], destination) && (protocol == 6 || protocol == 17) && port == 53, nil
}
