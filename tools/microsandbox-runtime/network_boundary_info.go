package microsandboxruntime

import "slices"

// NetworkEgressEndpoint describes one explicit environment allowlist entry.
// Destination is an IPv4 CIDR or the native SDK's local host transport.
type NetworkEgressEndpoint struct {
	Destination string
	Protocol    string
	Ports       string
}

// NetworkBoundaryInfo is a read-only snapshot of the validated environment
// boundary. An empty fingerprint means that isolation is disabled.
type NetworkBoundaryInfo struct {
	Fingerprint    string
	AllowedEgress  []NetworkEgressEndpoint
	GlobalEgress   []NetworkEgressEndpoint
	DNSNameservers []string
}

// NetworkBoundary returns independent copies, so consumers cannot change the
// policy used to launch, capture or restore guests.
func (c *Client) NetworkBoundary() NetworkBoundaryInfo {
	if c.boundary == nil {
		return NetworkBoundaryInfo{}
	}
	info := NetworkBoundaryInfo{
		Fingerprint: c.boundary.fingerprint, DNSNameservers: slices.Clone(c.boundary.Nameservers),
		AllowedEgress: make([]NetworkEgressEndpoint, 0, len(c.boundary.Allowed)),
	}
	for _, endpoint := range c.boundary.Allowed {
		info.AllowedEgress = append(info.AllowedEgress, NetworkEgressEndpoint(endpoint))
	}
	for _, endpoint := range c.boundary.global {
		info.GlobalEgress = append(info.GlobalEgress, NetworkEgressEndpoint(endpoint))
	}
	return info
}
