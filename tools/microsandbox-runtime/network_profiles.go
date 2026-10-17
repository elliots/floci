package microsandboxruntime

import "net/netip"

var privateSDKNetworks = []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10"}

var nonPublicSDKNetworks = []string{
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
	"169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16",
	"198.18.0.0/15", "224.0.0.0/4", "240.0.0.0/4",
}

// Public/private selectors can bisect a configured global CIDR. Split before
// intersecting ports so neither named group becomes a broader explicit CIDR.
func profileEndpoints(profile string, endpoint networkEndpoint) []networkEndpoint {
	if profile != "public" && profile != "private" {
		return []networkEndpoint{endpoint}
	}
	prefix, err := netip.ParsePrefix(endpoint.Destination)
	if err != nil || !prefix.Addr().Is4() {
		return nil
	}
	var prefixes []netip.Prefix
	if profile == "private" {
		for _, value := range privateSDKNetworks {
			private := netip.MustParsePrefix(value)
			if prefix.Overlaps(private) {
				if prefix.Bits() > private.Bits() {
					private = prefix.Masked()
				}
				prefixes = append(prefixes, private)
			}
		}
	} else {
		prefixes = []netip.Prefix{prefix.Masked()}
		for _, value := range nonPublicSDKNetworks {
			excluded := netip.MustParsePrefix(value)
			var remaining []netip.Prefix
			for _, candidate := range prefixes {
				remaining = append(remaining, subtractNetwork(candidate, excluded)...)
			}
			prefixes = remaining
		}
	}
	result := make([]networkEndpoint, 0, len(prefixes))
	for _, prefix := range prefixes {
		candidate := endpoint
		candidate.Destination = prefix.String()
		result = append(result, candidate)
	}
	return result
}

func subtractNetwork(prefix, excluded netip.Prefix) []netip.Prefix {
	if !prefix.Overlaps(excluded) {
		return []netip.Prefix{prefix}
	}
	if excluded.Bits() <= prefix.Bits() {
		return nil
	}
	bits := prefix.Bits() + 1
	left := netip.PrefixFrom(prefix.Addr(), bits)
	address := prefix.Addr().As4()
	address[(bits-1)/8] |= 1 << (7 - (bits-1)%8)
	right := netip.PrefixFrom(netip.AddrFrom4(address), bits)
	return append(subtractNetwork(left, excluded), subtractNetwork(right, excluded)...)
}

func privateSDKPrefix(prefix netip.Prefix) bool {
	if !prefix.IsValid() || !prefix.Addr().Is4() {
		return false
	}
	for _, value := range privateSDKNetworks {
		private := netip.MustParsePrefix(value)
		if private.Bits() <= prefix.Bits() && private.Contains(prefix.Addr()) {
			return true
		}
	}
	return false
}
