package guestnet

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"testing"

	"github.com/mdlayher/netlink"
	"github.com/mdlayher/netlink/nltest"
)

func conntrackFixture(address string, protocol byte, port uint16) netlink.Message {
	parsed := netip.MustParseAddr(address).As4()
	attrs := nltest.MustMarshalAttributes([]netlink.Attribute{
		{Type: netlink.Nested | ctTupleOriginal, Data: nltest.MustMarshalAttributes([]netlink.Attribute{
			{Type: netlink.Nested | ctTupleIP, Data: nltest.MustMarshalAttributes([]netlink.Attribute{
				{Type: 1, Data: []byte{192, 168, 1, 2}}, {Type: ctIPv4Dst, Data: parsed[:]},
			})},
			{Type: netlink.Nested | ctTupleProtocol, Data: nltest.MustMarshalAttributes([]netlink.Attribute{
				{Type: ctProtocolNum, Data: []byte{protocol}}, {Type: 2, Data: []byte{0xa0, 0x11}},
				{Type: ctProtocolDst, Data: binary.BigEndian.AppendUint16(nil, port)},
			})},
		})},
		{Type: ctID, Data: []byte{1, 2, 3, 4}},
		{Type: ctZone, Data: []byte{0, 2}},
		{Type: 7, Data: []byte{0, 0, 0, 30}},
	})
	return netlink.Message{Header: netlink.Header{Type: 0x100}, Data: append([]byte{2, 0, 0, 0}, attrs...)}
}

func TestDNSConntrackDeletesOnlyOwnedDNSFlows(t *testing.T) {
	for _, address := range []string{"198.18.0.53", "198.18.0.54", "198.18.0.55", "198.18.0.56", "10.1.2.3"} {
		for _, protocol := range []byte{6, 17, 1} {
			for _, port := range []uint16{53, 443} {
				message := conntrackFixture(address, protocol, port)
				request, matched, err := dnsConntrackDelete(message)
				want := address != "198.18.0.56" && address != "10.1.2.3" && protocol != 1 && port == 53
				if err != nil || matched != want {
					t.Fatalf("%s/%d/%d: matched=%v err=%v", address, protocol, port, matched, err)
				}
				if !want {
					continue
				}
				if request.Header.Type != 0x102 || request.Header.Flags != netlink.Request|netlink.Acknowledge {
					t.Fatal("deletion must request a kernel acknowledgement")
				}
				attributes, err := netlink.UnmarshalAttributes(request.Data[4:])
				if err != nil || len(attributes) != 3 {
					t.Fatalf("deletion must retain exactly the original tuple, flow ID and zone: %v", err)
				}
				original, _ := netlink.UnmarshalAttributes(message.Data[4:])
				for index, attribute := range attributes {
					if attribute.Type != original[index].Type || !bytes.Equal(attribute.Data, original[index].Data) {
						t.Fatal("deletion changed original identity")
					}
				}
			}
		}
	}
}

func TestDNSConntrackRequiresFlowIDAndRejectsMalformedAttributes(t *testing.T) {
	message := conntrackFixture("198.18.0.53", 17, 53)
	attributes, _ := netlink.UnmarshalAttributes(message.Data[4:])
	message.Data = append([]byte{2, 0, 0, 0}, nltest.MustMarshalAttributes(attributes[:1])...)
	if _, _, err := dnsConntrackDelete(message); err == nil {
		t.Fatal("deleting an unfenced flow must fail")
	}
	message.Data = []byte{2, 0, 0, 0, 255, 255, 0, 0}
	if _, _, err := dnsConntrackDelete(message); err == nil {
		t.Fatal("malformed attributes accepted")
	}
	message.Data = []byte{10, 0, 0, 0}
	if _, matched, err := dnsConntrackDelete(message); err != nil || matched {
		t.Fatal("IPv6 flows must remain untouched")
	}
}

func FuzzDNSConntrackDelete(f *testing.F) {
	f.Add(conntrackFixture("198.18.0.53", 17, 53).Data)
	f.Add([]byte{2, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		request, matched, err := dnsConntrackDelete(netlink.Message{Data: data})
		if matched && (err != nil || len(request.Data) <= 4 || request.Header.Type != 0x102) {
			t.Fatal("matched deletion must carry an exact tuple")
		}
	})
}
