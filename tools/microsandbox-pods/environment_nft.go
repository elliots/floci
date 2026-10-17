package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

var environmentTableName = regexp.MustCompile(`^floci_iso_[a-z0-9]+$`)
var environmentBridgeName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,15}$`)

// verifyEnvironmentNFT verifies the complete table emitted by Floci's Docker
// isolation compiler. It accepts live counter/handle values and other owners'
// exact tuple entries, but never additional statements or broader accept rules.
func verifyEnvironmentNFT(data []byte, table string, bridges []string, global []msbruntime.NetworkEgressEndpoint) error {
	if !environmentTableName.MatchString(table) || len(bridges) == 0 {
		return errors.New("invalid environment firewall identity")
	}
	for _, bridge := range bridges {
		if !environmentBridgeName.MatchString(bridge) {
			return errors.New("invalid protected bridge identity")
		}
	}
	expected, err := environmentRules(bridges, global)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil || !nftFields(document, "nftables") {
		return errors.New("invalid environment nft JSON")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("environment nft JSON contains extra data")
	}
	objects, ok := document["nftables"].([]any)
	if !ok {
		return errors.New("environment nft JSON has no object inventory")
	}
	seen := map[string]bool{}
	rules := map[string][][]any{}
	for _, item := range objects {
		object, ok := item.(map[string]any)
		if !ok || len(object) != 1 {
			return errors.New("unexpected environment nft object")
		}
		for kind, value := range object {
			entry, ok := value.(map[string]any)
			if !ok {
				return errors.New("invalid environment nft object body")
			}
			if kind == "metainfo" {
				if seen[kind] {
					return errors.New("duplicate nft metadata")
				}
				seen[kind] = true
				continue
			}
			if entry["family"] != "inet" || kind != "table" && entry["table"] != table {
				return errors.New("environment nft object belongs to another table")
			}
			if handle, present := entry["handle"]; present && !nftUnsigned(handle, false) {
				return errors.New("invalid environment nft handle")
			}
			name, _ := entry["name"].(string)
			key := kind + "/" + name
			if kind != "rule" {
				if seen[key] {
					return errors.New("duplicate environment nft object")
				}
				seen[key] = true
			}
			switch kind {
			case "table":
				if name != table || !nftAllowedFields(entry, "family", "name", "handle", "flags") || !nftEmptyFlags(entry) {
					return errors.New("unexpected environment nft table")
				}
			case "chain":
				if err := verifyEnvironmentChain(entry, name); err != nil {
					return err
				}
			case "set":
				if err := verifyEnvironmentSet(entry, name); err != nil {
					return err
				}
			case "rule":
				chain, ok := entry["chain"].(string)
				if !ok || expected[chain] == nil || !nftAllowedFields(entry, "family", "table", "chain", "handle", "expr") {
					return errors.New("unexpected environment nft rule")
				}
				expressions, ok := entry["expr"].([]any)
				if !ok {
					return errors.New("missing environment nft rule expressions")
				}
				normalized, err := normalizeEnvironmentExpressions(expressions)
				if err != nil {
					return err
				}
				rules[chain] = append(rules[chain], normalized)
			default:
				return fmt.Errorf("unsupported environment nft object: %s", kind)
			}
		}
	}
	for _, key := range []string{"table/" + table, "chain/forward", "chain/input", "chain/boundary", "set/native_tcp", "set/native_udp"} {
		if !seen[key] {
			return fmt.Errorf("missing environment nft object: %s", key)
		}
	}
	for chain, want := range expected {
		if !reflect.DeepEqual(rules[chain], want) {
			return fmt.Errorf("environment firewall %s rules do not match the declared boundary", chain)
		}
	}
	return nil
}

func verifyEnvironmentChain(entry map[string]any, name string) error {
	if name == "boundary" {
		if !nftAllowedFields(entry, "family", "table", "name", "handle") {
			return errors.New("environment boundary must be a regular chain")
		}
		return nil
	}
	if name != "forward" && name != "input" || !nftAllowedFields(entry, "family", "table", "name", "handle", "type", "hook", "prio", "policy") ||
		entry["type"] != "filter" || entry["hook"] != name || entry["prio"] != json.Number("-200") || entry["policy"] != "accept" {
		return errors.New("unexpected environment firewall hook or chain")
	}
	return nil
}

func verifyEnvironmentSet(entry map[string]any, name string) error {
	if name != "native_tcp" && name != "native_udp" || !nftAllowedFields(entry, "family", "table", "name", "handle", "type", "elem", "flags") || !nftEmptyFlags(entry) ||
		!reflect.DeepEqual(entry["type"], []any{"ipv4_addr", "ipv4_addr", "inet_service"}) {
		return errors.New("unexpected environment native tuple set")
	}
	if elements, present := entry["elem"]; present {
		values, ok := elements.([]any)
		if !ok {
			return errors.New("invalid environment native tuple elements")
		}
		for _, value := range values {
			element, ok := value.(map[string]any)
			if !ok || !nftFields(element, "concat") {
				return errors.New("native tuple entry is not an exact concatenation")
			}
			parts, ok := element["concat"].([]any)
			if !ok || len(parts) != 3 {
				return errors.New("native tuple entry has unexpected geometry")
			}
			for _, part := range parts[:2] {
				text, ok := part.(string)
				address, err := netip.ParseAddr(text)
				if !ok || err != nil || !address.Is4() || address.String() != text || address.IsUnspecified() || address.IsMulticast() {
					return errors.New("native tuple entry must contain exact unicast IPv4 addresses")
				}
			}
			if _, err := nftPort(parts[2]); err != nil {
				return err
			}
		}
	}
	return nil
}

func environmentRules(bridges []string, global []msbruntime.NetworkEgressEndpoint) (map[string][][]any, error) {
	interfaces := slices.Clone(bridges)
	slices.Sort(interfaces)
	interfaces = slices.Compact(interfaces)
	match := func(left any, right any) any {
		return map[string]any{"match": map[string]any{"op": "==", "left": left, "right": right}}
	}
	state := func(values ...string) any {
		return map[string]any{"match": map[string]any{"op": "in", "left": map[string]any{"ct": map[string]any{"key": "state"}}, "right": values}}
	}
	meta := func(key string) any { return map[string]any{"meta": map[string]any{"key": key}} }
	payload := func(protocol, field string) any {
		return map[string]any{"payload": map[string]any{"protocol": protocol, "field": field}}
	}
	accept := map[string]any{"accept": nil}
	jump := []any{match(meta("iifname"), interfaces), map[string]any{"jump": map[string]any{"target": "boundary"}}}
	rules := map[string][][]any{
		"forward": {jump}, "input": {jump},
		"boundary": {
			{state("new"), map[string]any{"mangle": map[string]any{"key": meta("nftrace"), "value": json.Number("1")}}},
			{match(map[string]any{"ct": map[string]any{"key": "direction"}}, "reply"), state("established", "related"), accept},
			{match(meta("oifname"), interfaces), accept},
		},
	}
	for _, protocol := range []string{"tcp", "udp"} {
		tuple := map[string]any{"concat": []any{payload("ip", "saddr"), payload("ip", "daddr"), payload(protocol, "dport")}}
		rules["boundary"] = append(rules["boundary"], []any{match(tuple, "@native_"+protocol), accept})
	}
	for _, endpoint := range global {
		prefix, err := netip.ParsePrefix(endpoint.Destination)
		if err != nil || !prefix.Addr().Is4() || prefix != prefix.Masked() || endpoint.Protocol != "tcp" && endpoint.Protocol != "udp" {
			return nil, errors.New("invalid expected global environment rule")
		}
		ports := strings.Split(endpoint.Ports, "-")
		if len(ports) > 2 {
			return nil, errors.New("invalid expected environment port range")
		}
		low, err := nftPort(json.Number(ports[0]))
		if err != nil {
			return nil, err
		}
		high := low
		if len(ports) == 2 {
			high, err = nftPort(json.Number(ports[1]))
			if err != nil || high < low {
				return nil, errors.New("invalid expected environment port range")
			}
		}
		rules["boundary"] = append(rules["boundary"], []any{match(payload("ip", "daddr"), prefix.String()), match(payload(endpoint.Protocol, "dport"), [2]int{low, high}), accept})
	}
	rules["boundary"] = append(rules["boundary"], []any{map[string]any{"counter": map[string]any{}}, map[string]any{"drop": nil}})
	return rules, nil
}

func normalizeEnvironmentExpressions(expressions []any) ([]any, error) {
	result := make([]any, 0, len(expressions))
	for _, expression := range expressions {
		object, ok := expression.(map[string]any)
		if !ok || len(object) != 1 {
			return nil, errors.New("invalid environment firewall expression")
		}
		if value, present := object["counter"]; present {
			counter, ok := value.(map[string]any)
			if !ok || !nftFields(counter, "packets", "bytes") || !nftUnsigned(counter["packets"], true) || !nftUnsigned(counter["bytes"], true) {
				return nil, errors.New("invalid environment firewall counter")
			}
			object["counter"] = map[string]any{}
		}
		if value, present := object["match"]; present {
			match, ok := value.(map[string]any)
			if !ok || !nftFields(match, "op", "left", "right") {
				return nil, errors.New("invalid environment firewall match")
			}
			left, ok := match["left"].(map[string]any)
			if !ok || len(left) != 1 {
				return nil, errors.New("invalid environment firewall match operand")
			}
			if meta, ok := left["meta"].(map[string]any); ok && (meta["key"] == "iifname" || meta["key"] == "oifname") {
				values, err := nftStringSet(match["right"])
				if err != nil {
					return nil, err
				}
				match["right"] = values
			}
			if ct, ok := left["ct"].(map[string]any); ok && ct["key"] == "state" {
				values, err := nftStringSet(match["right"])
				if err != nil {
					return nil, err
				}
				match["right"] = values
			}
			if payload, ok := left["payload"].(map[string]any); ok {
				switch payload["field"] {
				case "daddr":
					prefix, err := nftPrefix(match["right"])
					if err != nil {
						return nil, err
					}
					match["right"] = prefix
				case "dport":
					ports, err := nftPortRange(match["right"])
					if err != nil {
						return nil, err
					}
					match["right"] = ports
				}
			}
		}
		result = append(result, object)
	}
	return result, nil
}

func nftStringSet(value any) ([]string, error) {
	if object, ok := value.(map[string]any); ok {
		if !nftFields(object, "set") {
			return nil, errors.New("unexpected nft set expression")
		}
		value = object["set"]
	}
	var result []string
	switch values := value.(type) {
	case string:
		result = []string{values}
	case []any:
		for _, value := range values {
			text, ok := value.(string)
			if !ok {
				return nil, errors.New("unexpected nft set member")
			}
			result = append(result, text)
		}
	default:
		return nil, errors.New("unexpected nft set value")
	}
	slices.Sort(result)
	return slices.Compact(result), nil
}

func nftPrefix(value any) (string, error) {
	if text, ok := value.(string); ok {
		address, err := netip.ParseAddr(text)
		if err == nil && address.Is4() && address.String() == text {
			return netip.PrefixFrom(address, 32).String(), nil
		}
	}
	object, ok := value.(map[string]any)
	if !ok || !nftFields(object, "prefix") {
		return "", errors.New("unexpected nft IPv4 prefix")
	}
	prefix, ok := object["prefix"].(map[string]any)
	if !ok || !nftFields(prefix, "addr", "len") {
		return "", errors.New("invalid nft IPv4 prefix")
	}
	address, _ := prefix["addr"].(string)
	bits, _ := prefix["len"].(json.Number)
	parsed, err := netip.ParsePrefix(address + "/" + string(bits))
	if err != nil || !parsed.Addr().Is4() || parsed != parsed.Masked() {
		return "", errors.New("invalid nft IPv4 network")
	}
	return parsed.String(), nil
}

func nftPortRange(value any) ([2]int, error) {
	if port, err := nftPort(value); err == nil {
		return [2]int{port, port}, nil
	}
	object, ok := value.(map[string]any)
	if !ok || !nftFields(object, "range") {
		return [2]int{}, errors.New("unexpected nft port range")
	}
	values, ok := object["range"].([]any)
	if !ok || len(values) != 2 {
		return [2]int{}, errors.New("invalid nft port range")
	}
	low, lowErr := nftPort(values[0])
	high, highErr := nftPort(values[1])
	if lowErr != nil || highErr != nil || high < low {
		return [2]int{}, errors.New("invalid nft port range bounds")
	}
	return [2]int{low, high}, nil
}

func nftPort(value any) (int, error) {
	number, ok := value.(json.Number)
	port, err := strconv.Atoi(string(number))
	if !ok || err != nil || port < 1 || port > 65535 || strconv.Itoa(port) != string(number) {
		return 0, errors.New("native tuple or rule port must be an exact integer in 1..65535")
	}
	return port, nil
}

func nftUnsigned(value any, zero bool) bool {
	number, ok := value.(json.Number)
	parsed, err := strconv.ParseUint(string(number), 10, 64)
	return ok && err == nil && (zero || parsed > 0) && strconv.FormatUint(parsed, 10) == string(number)
}

func nftFields(object map[string]any, fields ...string) bool {
	return len(object) == len(fields) && nftAllowedFields(object, fields...)
}

func nftAllowedFields(object map[string]any, fields ...string) bool {
	for key := range object {
		if !slices.Contains(fields, key) {
			return false
		}
	}
	return true
}

func nftEmptyFlags(object map[string]any) bool {
	flags, present := object["flags"]
	if !present {
		return true
	}
	values, ok := flags.([]any)
	return ok && len(values) == 0
}
