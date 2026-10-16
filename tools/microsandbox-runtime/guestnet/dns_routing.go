package guestnet

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
)

// DNSRouting sends guest DNS directly to explicitly configured resolvers. The
// Linux installer exits after installing kernel rules; it starts no server.
type DNSRouting struct {
	Nameservers []string `json:"nameservers"`
}

type dnsRoute struct {
	Virtual netip.Addr
	Target  netip.AddrPort
}

var dnsVirtualAddresses = [...]netip.Addr{
	netip.MustParseAddr("198.18.0.53"),
	netip.MustParseAddr("198.18.0.54"),
	netip.MustParseAddr("198.18.0.55"),
}

func planDNSRouting(config DNSRouting, hostsPath string) ([]dnsRoute, error) {
	if len(config.Nameservers) < 1 || len(config.Nameservers) > len(dnsVirtualAddresses) {
		return nil, errors.New("guest DNS routing requires one to three explicit nameservers")
	}
	routes := make([]dnsRoute, 0, len(config.Nameservers))
	for index, endpoint := range config.Nameservers {
		host, portText, err := net.SplitHostPort(endpoint)
		if err != nil {
			return nil, errors.New("guest DNS nameserver requires an explicit IPv4 address and port")
		}
		port, err := strconv.ParseUint(portText, 10, 16)
		if err != nil || port == 0 {
			return nil, errors.New("guest DNS nameserver port must be between 1 and 65535")
		}
		if slices.Contains([]uint64{53, 853, 5353, 5355, 137}, port) {
			return nil, errors.New("guest DNS nameserver port is reserved by the runtime DNS interceptor")
		}
		var address netip.Addr
		if host == "host.microsandbox.internal" {
			address, err = dnsHostAddress(hostsPath)
		} else {
			address, err = netip.ParseAddr(host)
		}
		if err != nil || !address.Is4() || !address.IsPrivate() {
			return nil, errors.New("guest DNS nameserver must be a private IPv4 address or the guest hosts mapping")
		}
		routes = append(routes, dnsRoute{Virtual: dnsVirtualAddresses[index], Target: netip.AddrPortFrom(address, uint16(port))})
	}
	return routes, nil
}

func dnsHostAddress(path string) (netip.Addr, error) {
	file, err := os.Open(path)
	if err != nil {
		return netip.Addr{}, err
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, 65537))
	if err != nil || len(content) > 65536 {
		return netip.Addr{}, errors.New("guest hosts file is unreadable or too large")
	}
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	var found netip.Addr
	for scanner.Scan() {
		line, _, _ := strings.Cut(scanner.Text(), "#")
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		for _, name := range fields[1:] {
			if name != "host.microsandbox.internal" {
				continue
			}
			address, err := netip.ParseAddr(fields[0])
			if err != nil {
				return netip.Addr{}, errors.New("guest hosts mapping contains an invalid address")
			}
			if address.Is6() {
				continue
			}
			if !address.IsPrivate() {
				return netip.Addr{}, errors.New("guest hosts mapping must contain a private IPv4 address")
			}
			if found.IsValid() && found != address {
				return netip.Addr{}, errors.New("guest hosts mapping is ambiguous")
			}
			found = address
		}
	}
	if scanner.Err() != nil || !found.IsValid() {
		return netip.Addr{}, errors.New("guest hosts mapping is missing or invalid")
	}
	return found, nil
}

func dnsResolverConfiguration(existing []byte, routes []dnsRoute) ([]byte, error) {
	if len(existing) > 65536 {
		return nil, errors.New("guest resolver configuration is too large")
	}
	const marker = "# Configured by Floci guest DNS routing."
	var output strings.Builder
	output.WriteString(marker + "\n")
	for _, route := range routes {
		fmt.Fprintf(&output, "nameserver %s\n", route.Virtual)
	}
	for line := range strings.SplitSeq(string(existing), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || strings.TrimSpace(line) == marker || fields[0] == "nameserver" {
			continue
		}
		output.WriteString(line + "\n")
	}
	return []byte(output.String()), nil
}
