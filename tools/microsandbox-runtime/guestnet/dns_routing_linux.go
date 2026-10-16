package guestnet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/google/nftables"
	"github.com/google/nftables/expr"
	"github.com/mdlayher/netlink"
	"golang.org/x/sys/unix"
)

const dnsTable = "floci_dns"

// ConfigureDNS replaces only the owned DNS NAT table, expires its previous
// conntrack mappings, and updates resolv.conf. It must run again after restore
// because the VM host gateway can change while guest kernel state survives.
func ConfigureDNS(config DNSRouting) error {
	routes, err := planDNSRouting(config, "/etc/hosts")
	if err != nil {
		return err
	}
	lock, err := os.OpenFile("/run/floci-dns-routing.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("guest DNS routing is already being configured: %w", err)
	}
	resolver, err := os.OpenFile("/etc/resolv.conf", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("cannot open guest resolver configuration: %w", err)
	}
	defer resolver.Close()
	existing, err := io.ReadAll(io.LimitReader(resolver, 65537))
	if err != nil {
		return err
	}
	content, err := dnsResolverConfiguration(existing, routes)
	if err != nil {
		return err
	}
	connection, err := nftables.New(nftables.WithSockOptions(func(connection *netlink.Conn) error {
		return connection.SetDeadline(time.Now().Add(5 * time.Second))
	}))
	if err != nil {
		return err
	}
	tables, err := connection.ListTablesOfFamily(nftables.TableFamilyIPv4)
	if err != nil {
		return fmt.Errorf("cannot inspect guest DNS rules: %w", err)
	}
	queueDNSRules(connection, tables, routes)
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("cannot configure guest DNS rules: %w", err)
	}
	if err := clearDNSConntrack(); err != nil {
		return fmt.Errorf("cannot expire previous guest DNS routes: %w", err)
	}
	if _, err := resolver.WriteAt(content, 0); err != nil {
		return err
	}
	if err := resolver.Truncate(int64(len(content))); err != nil {
		return err
	}
	return resolver.Sync()
}

func queueDNSRules(connection *nftables.Conn, tables []*nftables.Table, routes []dnsRoute) {
	for _, table := range tables {
		if table.Name == dnsTable && table.Family == nftables.TableFamilyIPv4 {
			connection.DelTable(table)
		}
	}
	table := connection.AddTable(&nftables.Table{Name: dnsTable, Family: nftables.TableFamilyIPv4})
	chain := connection.AddChain(&nftables.Chain{
		Name: "output", Table: table, Type: nftables.ChainTypeNAT,
		Hooknum: nftables.ChainHookOutput, Priority: nftables.ChainPriorityNATDest,
	})
	for _, route := range routes {
		for _, protocol := range []byte{unix.IPPROTO_UDP, unix.IPPROTO_TCP} {
			virtual, target := route.Virtual.As4(), route.Target.Addr().As4()
			connection.AddRule(&nftables.Rule{Table: table, Chain: chain, Exprs: []expr.Any{
				&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 16, Len: 4},
				&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: virtual[:]},
				&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
				&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{protocol}},
				&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 2, Len: 2},
				&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{0, 53}},
				&expr.Immediate{Register: 1, Data: target[:]},
				&expr.Immediate{Register: 2, Data: binary.BigEndian.AppendUint16(nil, route.Target.Port())},
				&expr.NAT{Type: expr.NATTypeDestNAT, Family: unix.NFPROTO_IPV4, RegAddrMin: 1, RegProtoMin: 2},
			}})
		}
	}
}

func clearDNSConntrack() error {
	connection, err := netlink.Dial(unix.NETLINK_NETFILTER, nil)
	if err != nil {
		return err
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	entries, err := connection.Execute(netlink.Message{
		Header: netlink.Header{Type: 0x101, Flags: netlink.Request | netlink.Dump},
		Data:   []byte{unix.AF_INET, 0, 0, 0},
	})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		request, matches, err := dnsConntrackDelete(entry)
		if err != nil {
			return err
		}
		if !matches {
			continue
		}
		if _, err := connection.Execute(request); err != nil && !errors.Is(err, unix.ENOENT) {
			return err
		}
	}
	return nil
}
