package microsandboxruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	msb "github.com/superradcompany/microsandbox/sdk/go"
	"go.yaml.in/yaml/v3"
)

const networkPolicyLabel = "floci.network.policy"
const networkCAPath = "/etc/floci/certs/ca.crt"

type nativeNetworkBoundary struct {
	Allowed     []networkEndpoint `yaml:"allowed-egress"`
	Nameservers []string          `yaml:"dns-nameservers"`
	CAFile      string            `yaml:"ca-file"`
	fingerprint string
	caBytes     []byte
	guestAgent  []byte
	global      []networkEndpoint
}

type networkEndpoint struct {
	Destination string `yaml:"destination"`
	Protocol    string `yaml:"protocol"`
	Ports       string `yaml:"ports"`
}

// The same file configures Floci's outer Docker boundary and the native SDK.
// Global and native egress settings are read here; Floci validates simulator
// routes and the outer Docker boundary.
func readNetworkBoundary(file string) (*nativeNetworkBoundary, error) {
	if file == "" {
		return nil, nil
	}
	input, err := os.Open(file)
	if err != nil {
		return nil, errors.New("cannot read workload network configuration")
	}
	defer input.Close()
	data, err := io.ReadAll(io.LimitReader(input, 1<<20+1))
	if err != nil || len(data) > 1<<20 {
		return nil, errors.New("workload network configuration exceeds 1 MiB or cannot be read")
	}
	var document struct {
		Network struct {
			Isolation struct {
				Enabled bool `yaml:"enabled"`
			} `yaml:"isolation"`
			Native  yaml.Node `yaml:"native"`
			Allowed yaml.Node `yaml:"allowed-egress"`
		} `yaml:"network"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&document); err != nil {
		return nil, errors.New("invalid workload network configuration")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("workload network configuration must contain one document")
	}
	if !document.Network.Isolation.Enabled {
		return nil, nil
	}
	if document.Network.Native.Kind == 0 {
		return nil, errors.New("native isolation requires network.native")
	}
	native, err := yaml.Marshal(&document.Network.Native)
	if err != nil {
		return nil, errors.New("invalid native network configuration")
	}
	boundary := &nativeNetworkBoundary{}
	reader := yaml.NewDecoder(bytes.NewReader(native))
	reader.KnownFields(true)
	if err := reader.Decode(boundary); err != nil {
		return nil, errors.New("invalid native network configuration fields")
	}
	if len(boundary.Nameservers) == 0 || len(boundary.Nameservers) > 3 {
		return nil, errors.New("native isolation requires one to three controlled DNS nameservers")
	}
	for _, endpoint := range boundary.Allowed {
		if endpoint.Destination != "host" {
			prefix, err := netip.ParsePrefix(endpoint.Destination)
			if err != nil || !privatePrefix(prefix) {
				return nil, errors.New("native allowed egress requires a private IPv4 CIDR or host")
			}
		}
		if endpoint.Protocol != "tcp" && endpoint.Protocol != "udp" {
			return nil, errors.New("native allowed egress requires TCP or UDP")
		}
		if _, _, err := portRange(endpoint.Ports); err != nil {
			return nil, errors.New("native allowed egress requires an explicit valid port range")
		}
	}
	if document.Network.Allowed.Kind != 0 {
		encoded, err := yaml.Marshal(&document.Network.Allowed)
		if err != nil {
			return nil, errors.New("invalid global allowed egress configuration")
		}
		var endpoints []networkEndpoint
		reader := yaml.NewDecoder(bytes.NewReader(encoded))
		reader.KnownFields(true)
		if err := reader.Decode(&endpoints); err != nil {
			return nil, errors.New("invalid global allowed egress fields")
		}
		for index, endpoint := range endpoints {
			prefix, err := netip.ParsePrefix(endpoint.Destination)
			if err != nil || !globalEgressPrefix(prefix) {
				return nil, errors.New("global allowed egress requires a canonical IPv4 CIDR without unspecified, loopback, link-local, multicast or reserved addresses")
			}
			if endpoint.Protocol != "tcp" && endpoint.Protocol != "udp" {
				return nil, errors.New("global allowed egress requires TCP or UDP")
			}
			ports, err := globalEgressPorts(endpoint.Ports)
			if err != nil {
				return nil, err
			}
			endpoints[index].Ports = ports
		}
		boundary.global = slices.Clone(endpoints)
		boundary.Allowed = append(boundary.Allowed, endpoints...)
	}
	for _, server := range boundary.Nameservers {
		address, err := netip.ParseAddrPort(server)
		if err != nil || address.Port() == 0 || !address.Addr().Is4() || !(address.Addr().IsPrivate() || address.Addr().IsLoopback()) {
			return nil, errors.New("native DNS nameservers must be local IPv4:port endpoints")
		}
		if slices.Contains([]uint16{53, 137, 853, 5353, 5355}, address.Port()) {
			return nil, errors.New("native DNS needs a direct resolver port; 53, 137, 853, 5353 and 5355 are intercepted or blocked by the SDK")
		}
	}
	if boundary.CAFile != "" {
		if !filepath.IsAbs(boundary.CAFile) {
			return nil, errors.New("native CA file must use an absolute host path")
		}
		data, err := os.ReadFile(boundary.CAFile)
		if err != nil || !publicCertificates(data) {
			return nil, errors.New("native CA file must contain public certificates only")
		}
		boundary.caBytes = data
	}
	digest := sha256.Sum256(data)
	boundary.fingerprint = hex.EncodeToString(digest[:])
	return boundary, nil
}

func publicCertificates(data []byte) bool {
	count := 0
	for len(bytes.TrimSpace(data)) > 0 {
		data = bytes.TrimSpace(data)
		if !bytes.HasPrefix(data, []byte("-----BEGIN CERTIFICATE-----")) {
			return false
		}
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return false
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return false
		}
		data = rest
		count++
	}
	return count > 0
}

func privatePrefix(prefix netip.Prefix) bool {
	if !prefix.Addr().Is4() {
		return false
	}
	for _, candidate := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.0/8"} {
		outer := netip.MustParsePrefix(candidate)
		if outer.Bits() <= prefix.Bits() && outer.Contains(prefix.Addr()) {
			return true
		}
	}
	return false
}

// Public SDK rules must never become an explicit CIDR rule spanning a more
// privileged destination group. Keep the entire prefix outside local/reserved
// ranges, including the SDK's possible benchmark-range host gateway.
func publicPrefix(prefix netip.Prefix) bool {
	if !prefix.IsValid() || !prefix.Addr().Is4() {
		return false
	}
	for _, excluded := range nonPublicSDKNetworks {
		if prefix.Overlaps(netip.MustParsePrefix(excluded)) {
			return false
		}
	}
	return true
}

func globalEgressPrefix(prefix netip.Prefix) bool {
	if !prefix.IsValid() || !prefix.Addr().Is4() || prefix.Bits() == 0 || prefix != prefix.Masked() {
		return false
	}
	for _, excluded := range []string{"0.0.0.0/8", "127.0.0.0/8", "169.254.0.0/16", "224.0.0.0/4", "240.0.0.0/4"} {
		if prefix.Overlaps(netip.MustParsePrefix(excluded)) {
			return false
		}
	}
	return true
}

func globalEgressPorts(value string) (string, error) {
	low, high, err := portRange(value)
	if err != nil {
		return "", errors.New("global allowed egress requires an explicit valid port range")
	}
	for _, part := range strings.Split(value, "-") {
		number, err := strconv.Atoi(part)
		if err != nil || part != strconv.Itoa(number) {
			return "", errors.New("global allowed egress ports must use canonical positive decimal values")
		}
	}
	if low == high {
		return strconv.Itoa(low), nil
	}
	return strconv.Itoa(low) + "-" + strconv.Itoa(high), nil
}

func (b *nativeNetworkBoundary) restrict(original *msb.NetworkConfig) (*msb.NetworkConfig, error) {
	if original == nil {
		original = msb.NetworkPolicy.FromProfiles(msb.NetworkProfilePublic)
	}
	result := *original
	result.Rules = []msb.PolicyRule{msb.Rule.AllowDNS()}
	result.Rules = append(result.Rules, b.dnsRules()...)
	result.DefaultEgress = msb.PolicyActionDeny
	result.DNS = &msb.DNSConfig{Nameservers: slices.Clone(b.Nameservers), RebindProtection: new(false)}
	for _, rule := range original.Rules {
		if rule.Direction == msb.PolicyDirectionIngress {
			result.Rules = append(result.Rules, rule)
			continue
		}
		if rule.Direction != msb.PolicyDirectionEgress {
			return nil, invalid("native boundary requires explicit rule direction")
		}
		if rule.Action == msb.PolicyActionDeny {
			result.Rules = append(result.Rules, rule)
			continue
		}
		if rule.Action != msb.PolicyActionAllow {
			return nil, invalid("native boundary cannot interpret rule action")
		}
		for _, endpoint := range b.Allowed {
			for _, candidate := range profileEndpoints(rule.Destination, endpoint) {
				intersection, ok, err := intersectEndpoint(rule, candidate)
				if err != nil {
					return nil, err
				}
				if ok {
					result.Rules = append(result.Rules, intersection)
				}
			}
		}
	}
	if original.DefaultEgress == msb.PolicyActionAllow {
		for _, endpoint := range b.Allowed {
			result.Rules = append(result.Rules, msb.PolicyRule{Action: msb.PolicyActionAllow, Direction: msb.PolicyDirectionEgress, Destination: endpoint.Destination, Protocol: msb.PolicyProtocol(endpoint.Protocol), Port: endpoint.Ports})
		}
	}
	result.Rules = uniquePolicyRules(result.Rules)
	return &result, nil
}

func uniquePolicyRules(rules []msb.PolicyRule) []msb.PolicyRule {
	result := make([]msb.PolicyRule, 0, len(rules))
	for _, rule := range rules {
		if !slices.ContainsFunc(result, func(existing msb.PolicyRule) bool {
			return existing.Action == rule.Action && existing.Direction == rule.Direction && existing.Destination == rule.Destination &&
				existing.Protocol == rule.Protocol && existing.Port == rule.Port && slices.Equal(existing.Protocols, rule.Protocols) && slices.Equal(existing.Ports, rule.Ports)
		}) {
			result = append(result, rule)
		}
	}
	return result
}

func intersectEndpoint(rule msb.PolicyRule, endpoint networkEndpoint) (msb.PolicyRule, bool, error) {
	destination := endpoint.Destination
	switch rule.Destination {
	case "*":
	case "host":
		if destination != "host" {
			return rule, false, nil
		}
	case "private":
		prefix, _ := netip.ParsePrefix(destination)
		if !privateSDKPrefix(prefix) {
			return rule, false, nil
		}
	case "public":
		prefix, err := netip.ParsePrefix(destination)
		if err != nil || !publicPrefix(prefix) {
			return rule, false, nil
		}
	default:
		original, err := netip.ParsePrefix(rule.Destination)
		if err != nil {
			return rule, false, invalid("native boundary cannot interpret destination")
		}
		if destination == "host" {
			// Host transport is mapped by the SDK. Only an unrestricted IPv4
			// rule can be proven to contain that address before the guest exists.
			if original != netip.MustParsePrefix("0.0.0.0/0") {
				return rule, false, nil
			}
		} else {
			allowed, _ := netip.ParsePrefix(destination)
			if original.Bits() <= allowed.Bits() && original.Contains(allowed.Addr()) {
				destination = allowed.Masked().String()
			} else if allowed.Bits() <= original.Bits() && allowed.Contains(original.Addr()) {
				destination = original.Masked().String()
			} else {
				return rule, false, nil
			}
		}
	}
	protocol := msb.PolicyProtocol(endpoint.Protocol)
	if rule.Protocol != "" && rule.Protocol != protocol {
		return rule, false, nil
	}
	if len(rule.Protocols) > 0 && !slices.Contains(rule.Protocols, protocol) {
		return rule, false, nil
	}
	if len(rule.Ports) > 0 {
		return rule, false, invalid("native boundary requires individual port ranges")
	}
	lo, hi := 1, 65535
	if rule.Port != "" {
		var err error
		lo, hi, err = portRange(rule.Port)
		if err != nil {
			return rule, false, err
		}
	}
	allowedLo, allowedHi, err := portRange(endpoint.Ports)
	if err != nil {
		return rule, false, err
	}
	lo, hi = max(lo, allowedLo), min(hi, allowedHi)
	if lo > hi {
		return rule, false, nil
	}
	ports := strconv.Itoa(lo)
	if hi != lo {
		ports += "-" + strconv.Itoa(hi)
	}
	return msb.PolicyRule{Action: msb.PolicyActionAllow, Direction: msb.PolicyDirectionEgress, Destination: destination, Protocol: protocol, Port: ports}, true, nil
}

// NetworkPolicyFingerprint identifies the immutable environment boundary used
// by this client. Empty means isolation was not configured.
func (c *Client) NetworkPolicyFingerprint() string {
	if c.boundary == nil {
		return ""
	}
	return c.boundary.fingerprint
}

// GuestCAPath identifies the public CA copied into each guest before boot.
// Empty means no validated CA was configured for this environment boundary.
func (c *Client) GuestCAPath() string {
	if c.boundary == nil || c.boundary.CAFile == "" {
		return ""
	}
	return networkCAPath
}

// VerifyNetworkBoundary refuses to advertise an isolated runtime while an older
// unprotected guest is still running in its dedicated home. It does not stop or
// destroy that guest. Stopped guests remain removable but cannot be restarted.
func (c *Client) VerifyNetworkBoundary(ctx context.Context) error {
	if c.boundary == nil {
		return nil
	}
	if err := c.core.initialize(); err != nil {
		return safeError("network initialization", err)
	}
	cursor := ""
	seen := map[string]bool{}
	for {
		options := []msb.SandboxListOption{msb.WithListLimit(100)}
		if cursor != "" {
			options = append(options, msb.WithListCursor(cursor))
		}
		page, err := msb.ListSandboxesWith(ctx, options...)
		if err != nil {
			return safeError("network inventory", err)
		}
		for _, sandbox := range page.Sandboxes {
			if sandbox.Status() != msb.SandboxStatusStopped && sandbox.Status() != msb.SandboxStatusCrashed {
				if err := c.boundary.requireGuest(sandbox, c.config.Home); err != nil {
					return err
				}
			}
		}
		if page.NextCursor == nil || *page.NextCursor == "" {
			return nil
		}
		cursor = *page.NextCursor
		if seen[cursor] {
			return errors.New("network inventory repeated a cursor")
		}
		seen[cursor] = true
	}
}

func portRange(value string) (int, int, error) {
	a, b, ok := strings.Cut(value, "-")
	if !ok {
		b = a
	}
	lo, e := strconv.ParseUint(a, 10, 16)
	hi, f := strconv.ParseUint(b, 10, 16)
	if e != nil || f != nil || lo == 0 || lo > hi {
		return 0, 0, invalid("invalid network port range")
	}
	return int(lo), int(hi), nil
}

func (b *nativeNetworkBoundary) launchArguments(args []string, network *msb.NetworkConfig, home string) ([]string, error) {
	result := []string{args[0]}
	for i := 1; i < len(args); i++ {
		flag := args[i]
		if flag == "--" {
			result = append(result, args[i:]...)
			break
		}
		if !strings.HasPrefix(flag, "-") || flag == "-d" || flag == "--detach" {
			result = append(result, flag)
			continue
		}
		// parse has validated every launch option. Consume its value together
		// with the flag so literal init arguments cannot become host options.
		i++
		value := args[i]
		if flag == "--net" || flag == "--net-rule" || flag == "--net-default" {
			continue
		}
		if b.CAFile != "" && (flag == "--env" || flag == "-e") {
			key, _, _ := strings.Cut(value, "=")
			if key == "FLOCI_CA_BUNDLE" {
				return nil, invalid("FLOCI_CA_BUNDLE is reserved for the native public CA")
			}
		}
		result = append(result, flag, value)
	}
	options := []string{"--net-default-egress", "deny", "--net-default-ingress", string(network.DefaultIngress), "--label", networkPolicyLabel + "=" + b.fingerprint, "--no-dns-rebind-protection"}
	for _, rule := range network.Rules {
		protocols := rule.Protocols
		if len(protocols) == 0 {
			protocols = []msb.PolicyProtocol{rule.Protocol}
		}
		for _, protocol := range protocols {
			destination := rule.Destination
			if destination == "*" {
				destination = "any"
			}
			token := string(rule.Action) + ":" + string(rule.Direction) + "@" + destination
			if protocol != "" {
				token += ":" + string(protocol)
				if rule.Port != "" {
					token += ":" + rule.Port
				}
			}
			options = append(options, "--net-rule", token)
		}
	}
	for _, server := range b.Nameservers {
		options = append(options, "--dns-nameserver", server)
	}
	if b.CAFile != "" {
		publicCA, err := b.freezeCA(home)
		if err != nil {
			return nil, err
		}
		options = append(options, "--copy-file", publicCA+":"+networkCAPath, "--env", "FLOCI_CA_BUNDLE="+networkCAPath)
	}
	// Insert before positional image/command arguments, so no guest argument
	// can be mistaken for a runtime option.
	return append(append([]string{result[0]}, options...), result[1:]...), nil
}

func (b *nativeNetworkBoundary) requireIdentity(labels map[string]string) error {
	if labels[networkPolicyLabel] != b.fingerprint {
		return fmt.Errorf("native guest network boundary differs; recreate the guest under the current policy")
	}
	return nil
}

// Freeze only bytes validated when the boundary was loaded. The configured host
// path may change before a later CLI launch, but it is never copied into a guest.
func (b *nativeNetworkBoundary) freezeCA(home string) (string, error) {
	if len(b.caBytes) == 0 || !filepath.IsAbs(home) {
		return "", errors.New("native public CA was not validated or runtime home is invalid")
	}
	directory := filepath.Join(home, "floci-network-cas")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return "", errors.New("cannot prepare private native public CA directory")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return "", errors.New("native public CA directory must be private and cannot be a symlink")
	}
	digest := sha256.Sum256(b.caBytes)
	path := filepath.Join(directory, hex.EncodeToString(digest[:])+".pem")
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		pending, err := os.CreateTemp(directory, ".public-ca-")
		if err != nil {
			return "", errors.New("cannot prepare native public CA")
		}
		defer os.Remove(pending.Name())
		defer pending.Close()
		if _, err := pending.Write(b.caBytes); err != nil {
			return "", errors.New("cannot write native public CA")
		}
		if err := pending.Sync(); err != nil {
			return "", errors.New("cannot persist native public CA")
		}
		if err := pending.Close(); err != nil {
			return "", errors.New("cannot close native public CA")
		}
		// Linking publishes the completed file atomically without replacing a
		// concurrent launch's immutable copy or following an existing symlink.
		if err := os.Link(pending.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
			return "", errors.New("cannot publish native public CA")
		}
	} else if err != nil {
		return "", errors.New("cannot inspect native public CA")
	}
	info, err = os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return "", errors.New("native public CA copy must be a private regular file")
	}
	copied, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(copied, b.caBytes) {
		return "", errors.New("native public CA copy differs from the validated certificate")
	}
	return path, nil
}
