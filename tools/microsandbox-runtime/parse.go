package microsandboxruntime

import (
	"math"
	"net"
	"net/netip"
	"path"
	"strconv"
	"strings"
	"time"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

type command struct {
	op, name, image, selector, source, user             string
	argv                                                []string
	fallback, detached, noStdin, force, full, integrity bool
	timeout                                             *time.Duration
	config                                              msb.SandboxConfig
	stream                                              StreamOptions
	boundary                                            *nativeNetworkBoundary
}

func parse(args []string) (command, error) {
	var r command
	if len(args) == 0 {
		return r, invalid("missing operation")
	}
	for _, value := range args {
		if strings.ContainsRune(value, 0) {
			return r, invalid("argument contains a NUL byte")
		}
	}
	r.op = args[0]
	args = args[1:]
	if r.op == "snap" {
		if len(args) == 0 {
			return r, invalid("missing snapshot operation")
		}
		r.op += " " + args[0]
		args = args[1:]
	}
	allowed := map[string]map[string]bool{
		"--version": {}, "doctor": {},
		"inspect": {"--format": true}, "ls": {"--format": true}, "metrics": {"--format": true},
		"run":   {"--detach": false, "--name": true, "--cpus": true, "--memory": true, "--root-disk": true, "--mount-named": true, "--label": true, "--env": true, "--port": true, "--entrypoint": true, "--init": true, "--init-arg": true, "--init-env": true, "--net": true, "--net-rule": true, "--net-default": true},
		"exec":  {"--detach": false, "--no-tty": false, "--no-stdin": false, "--stream": false, "--user": true},
		"start": {}, "pause": {}, "resume": {},
		"stop": {"--force": false, "--timeout": true}, "rm": {"--force": false},
		"snap create": {"--sandbox": true, "--full": false, "--integrity": false},
		"snap verify": {}, "snap restore": {"--name": true, "--port": true, "--mount-named": true, "--net": true, "--net-default": true, "--net-rule": true},
		"snap ls": {"--format": true}, "snap rm": {"--force": false},
	}
	spec, ok := allowed[r.op]
	if !ok {
		return r, invalid("unsupported operation")
	}
	r.fallback = r.op == "--version" || r.op == "doctor" || r.op == "inspect" || r.op == "metrics" || r.op == "run"
	var positional, profiles, rules []string
	var netDefault string
	seen := map[string]bool{}
	for i := 0; i < len(args); i++ {
		token := args[i]
		if token == "--" {
			if r.op != "run" && r.op != "exec" {
				return r, invalid("unexpected command separator")
			}
			r.argv = append([]string(nil), args[i+1:]...)
			break
		}
		if !strings.HasPrefix(token, "-") {
			positional = append(positional, token)
			continue
		}
		flag := token
		switch flag {
		case "-d":
			flag = "--detach"
		case "-e":
			flag = "--env"
		case "-p":
			flag = "--port"
		case "-f":
			flag = "--force"
		case "-t":
			flag = "--timeout"
		}
		valueNeeded, ok := spec[flag]
		if !ok {
			return r, invalid("unsupported option")
		}
		if seen[flag] && flag != "--env" && flag != "--label" && flag != "--port" && flag != "--net-rule" && flag != "--net" && flag != "--init-arg" && flag != "--init-env" && flag != "--mount-named" {
			return r, invalid("duplicate option")
		}
		seen[flag] = true
		var value string
		if valueNeeded {
			i++
			if i >= len(args) || args[i] == "" {
				return r, invalid("missing option value")
			}
			value = args[i]
		}
		switch flag {
		case "--format":
			if value != "json" {
				return r, invalid("only JSON output is supported")
			}
		case "--detach":
			r.detached = true
		case "--no-stdin":
			r.noStdin = true
		case "--no-tty", "--stream":
		case "--name":
			r.name = value
		case "--sandbox":
			r.source = value
		case "--user":
			r.user = value
		case "--force":
			r.force = true
		case "--full":
			r.full = true
		case "--integrity":
			r.integrity = true
		case "--timeout":
			n, e := strconv.ParseUint(value, 10, 32)
			if e != nil {
				return r, invalid("invalid stop timeout")
			}
			d := time.Duration(n) * time.Second
			r.timeout = &d
		case "--cpus":
			n, e := strconv.ParseUint(value, 10, 8)
			if e != nil || n == 0 {
				return r, invalid("invalid CPU count")
			}
			r.config.CPUs = uint8(n)
		case "--memory":
			n, e := sizeMiB(value)
			if e != nil {
				return r, e
			}
			r.config.MemoryMiB = n
		case "--root-disk":
			flat := strings.HasPrefix(value, "flat:")
			value = strings.TrimPrefix(value, "flat:")
			n, e := sizeMiB(value)
			if e != nil {
				return r, e
			}
			d := msb.RootDisk.Managed(n)
			if flat {
				d = msb.RootDisk.Flat(msb.RootDiskFlatOptions{SizeMiB: n})
			}
			r.config.RootDisk = &d
		case "--mount-named":
			if err := parseNamedDiskMount(&r.config, value); err != nil {
				return r, err
			}
		case "--entrypoint":
			r.config.Entrypoint = []string{value}
		case "--init", "--init-arg", "--init-env":
			if r.config.Init == nil {
				r.config.Init = &msb.InitConfig{}
			}
			switch flag {
			case "--init":
				if value != "auto" && (!path.IsAbs(value) || path.Clean(value) != value) {
					return r, invalid("init must be auto or a clean absolute guest path")
				}
				r.config.Init.Cmd = value
			case "--init-arg":
				r.config.Init.Args = append(r.config.Init.Args, value)
			case "--init-env":
				key, setting, ok := strings.Cut(value, "=")
				if !ok || key == "" || strings.ContainsAny(key, " \t\r\n") {
					return r, invalid("init environment requires key=value")
				}
				if r.config.Init.Env == nil {
					r.config.Init.Env = map[string]string{}
				}
				r.config.Init.Env[key] = setting
			}
		case "--env", "--label":
			k, v, ok := strings.Cut(value, "=")
			if !ok || k == "" {
				return r, invalid("expected key=value")
			}
			if flag == "--env" {
				if r.config.Env == nil {
					r.config.Env = map[string]string{}
				}
				r.config.Env[k] = v
			} else {
				if r.config.Labels == nil {
					r.config.Labels = map[string]string{}
				}
				r.config.Labels[k] = v
			}
		case "--port":
			p, e := parsePort(value)
			if e != nil {
				return r, e
			}
			r.config.PortBindings = append(r.config.PortBindings, p)
		case "--net":
			profiles = append(profiles, strings.Split(value, ",")...)
		case "--net-rule":
			rules = append(rules, strings.Split(value, ",")...)
		case "--net-default":
			netDefault = value
		}
	}
	var err error
	r.config.Network, err = parseNetwork(profiles, rules, netDefault)
	if err != nil {
		return r, err
	}
	switch r.op {
	case "run":
		if !r.detached || r.name == "" || len(positional) != 1 {
			return r, invalid("run requires detached mode, name and image")
		}
		if r.config.Init != nil && r.config.Init.Cmd == "" {
			return r, invalid("init arguments require --init")
		}
		r.image = positional[0]
		r.config.Image = r.image
		r.config.Detached = true
		if len(r.argv) > 0 {
			r.config.Cmd = r.argv
		}
	case "exec":
		if len(positional) != 1 || len(r.argv) == 0 || r.argv[0] == "" {
			return r, invalid("exec requires sandbox and command after separator")
		}
		r.name = positional[0]
	case "start", "stop", "rm", "pause", "resume", "inspect":
		if len(positional) != 1 {
			return r, invalid("operation requires one sandbox")
		}
		r.name = positional[0]
	case "snap create":
		if len(positional) != 1 || r.source == "" {
			return r, invalid("snapshot creation requires name and source")
		}
		r.selector = positional[0]
	case "snap restore":
		if len(positional) != 1 || r.name == "" {
			return r, invalid("snapshot restore requires selector and name")
		}
		r.selector = positional[0]
	case "snap verify", "snap rm":
		if len(positional) != 1 {
			return r, invalid("snapshot operation requires one selector")
		}
		r.selector = positional[0]
	case "metrics":
		if len(positional) > 1 {
			return r, invalid("metrics accepts at most one sandbox")
		}
		if len(positional) == 1 {
			r.name = positional[0]
		}
	default:
		if len(positional) != 0 {
			return r, invalid("unexpected positional argument")
		}
	}
	return r, nil
}

func sizeMiB(value string) (uint32, error) {
	multiplier := uint64(1)
	for _, suffix := range []string{"GiB", "MiB", "G", "M"} {
		if strings.HasSuffix(value, suffix) {
			value = strings.TrimSuffix(value, suffix)
			if suffix == "G" || suffix == "GiB" {
				multiplier = 1024
			}
			break
		}
	}
	n, err := strconv.ParseUint(value, 10, 32)
	if err != nil || n == 0 || n > math.MaxUint32/multiplier {
		return 0, invalid("invalid memory or disk size")
	}
	return uint32(n * multiplier), nil
}

func parsePort(value string) (msb.PortBinding, error) {
	var p msb.PortBinding
	value, proto, hasProto := strings.Cut(value, "/")
	if !hasProto {
		proto = "tcp"
	}
	if proto != "tcp" && proto != "udp" {
		return p, invalid("unsupported published port protocol")
	}
	p.Protocol = msb.PortProtocol(proto)
	left, guest, ok := strings.Cut(value, ":")
	if !ok {
		return p, invalid("invalid published port")
	}
	if strings.Contains(guest, ":") {
		hostport, g, ok := strings.Cut(guest, ":")
		if !ok {
			return p, invalid("invalid published port")
		}
		p.Bind = left
		left = hostport
		guest = g
		addr, e := netip.ParseAddr(p.Bind)
		if e != nil || !addr.Is4() {
			return p, invalid("published bind must be an IPv4 address")
		}
	} else {
		p.Bind = "127.0.0.1"
	}
	h, e := strconv.ParseUint(left, 10, 16)
	g, f := strconv.ParseUint(guest, 10, 16)
	if e != nil || f != nil || h == 0 || g == 0 {
		return p, invalid("invalid published port")
	}
	p.HostPort, p.GuestPort = uint16(h), uint16(g)
	return p, nil
}

func parseNetwork(profiles, rules []string, action string) (*msb.NetworkConfig, error) {
	if len(profiles) == 0 && len(rules) == 0 && action == "" {
		return nil, nil
	}
	if len(profiles) > 0 && action != "" {
		return nil, invalid("network profiles conflict with default policy")
	}
	config := &msb.NetworkConfig{DefaultEgress: msb.PolicyActionDeny, DefaultIngress: msb.PolicyActionAllow}
	if len(profiles) > 0 {
		if len(profiles) == 1 && profiles[0] == "all" {
			config = msb.NetworkPolicy.AllowAll()
		} else if len(profiles) == 1 && profiles[0] == "none" {
			config = msb.NetworkPolicy.None()
		} else {
			ps := make([]msb.NetworkProfile, len(profiles))
			for i, p := range profiles {
				ps[i] = msb.NetworkProfile(strings.TrimSpace(p))
			}
			var err error
			config, err = msb.NetworkPolicy.FromProfilesChecked(ps...)
			if err != nil {
				return nil, invalid("invalid network profile")
			}
		}
	}
	if action != "" {
		if action != "allow" && action != "deny" {
			return nil, invalid("invalid network default")
		}
		config.DefaultEgress = msb.PolicyAction(action)
		config.DefaultIngress = msb.PolicyAction(action)
	}
	var parsed []msb.PolicyRule
	for _, raw := range rules {
		// This is the subset produced by Floci's IPv4 NIC policy compiler.
		a, tail, ok := strings.Cut(raw, ":")
		if !ok || (a != "allow" && a != "deny") {
			return nil, invalid("invalid network rule action")
		}
		direction, destination, ok := strings.Cut(tail, "@")
		if !ok || (direction != "egress" && direction != "ingress") {
			return nil, invalid("invalid network rule direction")
		}
		parts := strings.Split(destination, ":")
		if len(parts) > 3 {
			return nil, invalid("invalid network rule destination")
		}
		dest := parts[0]
		switch dest {
		case "any":
			dest = "*"
		case "host", "private", "public":
		default:
			if _, _, err := net.ParseCIDR(dest); err != nil {
				return nil, invalid("invalid network rule destination")
			}
		}
		rule := msb.PolicyRule{Action: msb.PolicyAction(a), Direction: msb.PolicyDirection(direction), Destination: dest}
		if len(parts) > 1 {
			if parts[1] != "tcp" && parts[1] != "udp" {
				return nil, invalid("invalid network rule protocol")
			}
			rule.Protocol = msb.PolicyProtocol(parts[1])
		}
		if len(parts) == 3 {
			lo, hi, ranged := strings.Cut(parts[2], "-")
			if !ranged {
				hi = lo
			}
			l, e := strconv.ParseUint(lo, 10, 16)
			h, f := strconv.ParseUint(hi, 10, 16)
			if e != nil || f != nil || l == 0 || l > h {
				return nil, invalid("invalid network rule ports")
			}
			rule.Port = parts[2]
		}
		parsed = append(parsed, rule)
	}
	config.Rules = append(parsed, config.Rules...)
	return config, nil
}
