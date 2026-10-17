package main

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

// Captured from nft 1.1.1 in the owned, network-isolated helper container.
const environmentNFTFixture = `{
 "nftables": [
  {"metainfo":{"version":"1.1.1","release_name":"Commodore Bullmoose #2","json_schema_version":1}},
  {"table":{"family":"inet","name":"floci_iso_sample","handle":1}},
  {"chain":{"family":"inet","table":"floci_iso_sample","name":"forward","handle":3,"type":"filter","hook":"forward","prio":-200,"policy":"accept"}},
  {"chain":{"family":"inet","table":"floci_iso_sample","name":"input","handle":4,"type":"filter","hook":"input","prio":-200,"policy":"accept"}},
  {"chain":{"family":"inet","table":"floci_iso_sample","name":"boundary","handle":5}},
  {"set":{"family":"inet","name":"native_tcp","table":"floci_iso_sample","type":["ipv4_addr","ipv4_addr","inet_service"],"handle":1,"elem":[{"concat":["10.0.0.1","10.0.0.2",4566]}]}},
  {"set":{"family":"inet","name":"native_udp","table":"floci_iso_sample","type":["ipv4_addr","ipv4_addr","inet_service"],"handle":2}},
  {"rule":{"family":"inet","table":"floci_iso_sample","chain":"forward","handle":7,"expr":[{"match":{"op":"==","left":{"meta":{"key":"iifname"}},"right":{"set":["br-one","br-two"]}}},{"jump":{"target":"boundary"}}]}},
  {"rule":{"family":"inet","table":"floci_iso_sample","chain":"input","handle":9,"expr":[{"match":{"op":"==","left":{"meta":{"key":"iifname"}},"right":{"set":["br-one","br-two"]}}},{"jump":{"target":"boundary"}}]}},
  {"rule":{"family":"inet","table":"floci_iso_sample","chain":"boundary","handle":10,"expr":[{"match":{"op":"in","left":{"ct":{"key":"state"}},"right":"new"}},{"mangle":{"key":{"meta":{"key":"nftrace"}},"value":1}}]}},
  {"rule":{"family":"inet","table":"floci_iso_sample","chain":"boundary","handle":11,"expr":[{"match":{"op":"==","left":{"ct":{"key":"direction"}},"right":"reply"}},{"match":{"op":"in","left":{"ct":{"key":"state"}},"right":["established","related"]}},{"accept":null}]}},
  {"rule":{"family":"inet","table":"floci_iso_sample","chain":"boundary","handle":13,"expr":[{"match":{"op":"==","left":{"meta":{"key":"oifname"}},"right":{"set":["br-one","br-two"]}}},{"accept":null}]}},
  {"rule":{"family":"inet","table":"floci_iso_sample","chain":"boundary","handle":14,"expr":[{"match":{"op":"==","left":{"concat":[{"payload":{"protocol":"ip","field":"saddr"}},{"payload":{"protocol":"ip","field":"daddr"}},{"payload":{"protocol":"tcp","field":"dport"}}]},"right":"@native_tcp"}},{"accept":null}]}},
  {"rule":{"family":"inet","table":"floci_iso_sample","chain":"boundary","handle":15,"expr":[{"match":{"op":"==","left":{"concat":[{"payload":{"protocol":"ip","field":"saddr"}},{"payload":{"protocol":"ip","field":"daddr"}},{"payload":{"protocol":"udp","field":"dport"}}]},"right":"@native_udp"}},{"accept":null}]}},
  {"rule":{"family":"inet","table":"floci_iso_sample","chain":"boundary","handle":16,"expr":[{"match":{"op":"==","left":{"payload":{"protocol":"ip","field":"daddr"}},"right":{"prefix":{"addr":"203.0.113.0","len":24}}}},{"match":{"op":"==","left":{"payload":{"protocol":"tcp","field":"dport"}},"right":{"range":[443,444]}}},{"accept":null}]}},
  {"rule":{"family":"inet","table":"floci_iso_sample","chain":"boundary","handle":17,"expr":[{"match":{"op":"==","left":{"payload":{"protocol":"ip","field":"daddr"}},"right":"198.51.100.5"}},{"match":{"op":"==","left":{"payload":{"protocol":"udp","field":"dport"}},"right":53}},{"accept":null}]}},
  {"rule":{"family":"inet","table":"floci_iso_sample","chain":"boundary","handle":18,"expr":[{"counter":{"packets":0,"bytes":0}},{"drop":null}]}}
 ]
}`

func environmentNFTGlobals() []msbruntime.NetworkEgressEndpoint {
	return []msbruntime.NetworkEgressEndpoint{
		{Destination: "203.0.113.0/24", Protocol: "tcp", Ports: "443-444"},
		{Destination: "198.51.100.5/32", Protocol: "udp", Ports: "53"},
	}
}

func TestEnvironmentNFTRealKernelContract(t *testing.T) {
	if err := verifyEnvironmentNFT([]byte(environmentNFTFixture), "floci_iso_sample", []string{"br-two", "br-one"}, environmentNFTGlobals()); err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentNFTTransportsPreservesExactForeignAndOwnedTuples(t *testing.T) {
	data := mutateEnvironmentNFT(t, func(objects []any) []any {
		environmentNFTEntry(objects, "set", "native_udp")["elem"] = []any{
			map[string]any{"concat": []any{"10.0.0.9", "192.168.0.1", json.Number("31000")}},
			map[string]any{"concat": []any{"10.0.0.1", "192.168.0.2", json.Number("32000")}},
		}
		return objects
	})
	tuples, err := environmentNFTTransports(data)
	if err != nil {
		t.Fatal(err)
	}
	want := []environmentNFTTransport{
		{Source: "10.0.0.1", Destination: "10.0.0.2", Protocol: "tcp", Port: 4566},
		{Source: "10.0.0.9", Destination: "192.168.0.1", Protocol: "udp", Port: 31000},
		{Source: "10.0.0.1", Destination: "192.168.0.2", Protocol: "udp", Port: 32000},
	}
	if !reflect.DeepEqual(tuples, want) {
		t.Fatalf("tuples %#v", tuples)
	}
	for _, change := range []func([]any) []any{
		func(objects []any) []any {
			environmentNFTEntry(objects, "set", "native_tcp")["elem"] = []any{map[string]any{"concat": []any{"10.0.0.0/8", "10.0.0.2", json.Number("443")}}}
			return objects
		},
		func(objects []any) []any {
			environmentNFTEntry(objects, "set", "native_udp")["flags"] = []any{"dynamic"}
			return objects
		},
		func(objects []any) []any {
			return append(objects, map[string]any{"set": environmentNFTEntry(objects, "set", "native_tcp")})
		},
	} {
		if _, err := environmentNFTTransports(mutateEnvironmentNFT(t, change)); err == nil {
			t.Fatal("unsafe tuple inventory accepted")
		}
	}
}

func mutateEnvironmentNFT(t *testing.T, change func([]any) []any) []byte {
	t.Helper()
	var document map[string]any
	decoder := json.NewDecoder(bytes.NewBufferString(environmentNFTFixture))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		t.Fatal(err)
	}
	document["nftables"] = change(document["nftables"].([]any))
	result, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func environmentNFTEntry(objects []any, kind, name string) map[string]any {
	for _, object := range objects {
		if entry, ok := object.(map[string]any)[kind].(map[string]any); ok && (name == "" || entry["name"] == name) {
			return entry
		}
	}
	panic("missing fixture object")
}

func environmentNFTRule(objects []any, chain string, index int) map[string]any {
	for _, object := range objects {
		if rule, ok := object.(map[string]any)["rule"].(map[string]any); ok && rule["chain"] == chain {
			if index == 0 {
				return rule
			}
			index--
		}
	}
	panic("missing fixture rule")
}

func TestEnvironmentNFTRejectsChangedBoundarySemantics(t *testing.T) {
	tests := map[string]func([]any) []any{
		"different family":   func(x []any) []any { environmentNFTEntry(x, "table", "")["family"] = "ip"; return x },
		"foreign table":      func(x []any) []any { environmentNFTEntry(x, "chain", "")["table"] = "other"; return x },
		"late hook":          func(x []any) []any { environmentNFTEntry(x, "chain", "forward")["prio"] = json.Number("0"); return x },
		"changed chain type": func(x []any) []any { environmentNFTEntry(x, "chain", "forward")["type"] = "nat"; return x },
		"missing input hook": func(x []any) []any { delete(environmentNFTEntry(x, "chain", "input"), "hook"); return x },
		"hooked boundary":    func(x []any) []any { environmentNFTEntry(x, "chain", "boundary")["hook"] = "output"; return x },
		"new chain": func(x []any) []any {
			return append(x, map[string]any{"chain": map[string]any{"family": "inet", "table": "floci_iso_sample", "name": "escape"}})
		},
		"missing set": func(x []any) []any {
			out := []any{}
			for _, o := range x {
				if e, ok := o.(map[string]any)["set"].(map[string]any); ok && e["name"] == "native_udp" {
					continue
				}
				out = append(out, o)
			}
			return out
		},
		"wrong tuple geometry": func(x []any) []any {
			environmentNFTEntry(x, "set", "native_tcp")["type"] = []any{"ipv4_addr", "inet_service"}
			return x
		},
		"interval set": func(x []any) []any {
			environmentNFTEntry(x, "set", "native_tcp")["flags"] = []any{"interval"}
			return x
		},
		"dynamic set": func(x []any) []any { environmentNFTEntry(x, "set", "native_tcp")["flags"] = []any{"dynamic"}; return x },
		"set timeout": func(x []any) []any {
			environmentNFTEntry(x, "set", "native_tcp")["timeout"] = json.Number("30")
			return x
		},
		"tuple source prefix": func(x []any) []any {
			environmentNFTEntry(x, "set", "native_tcp")["elem"] = []any{map[string]any{"concat": []any{map[string]any{"prefix": map[string]any{"addr": "10.0.0.0", "len": json.Number("8")}}, "10.0.0.2", json.Number("443")}}}
			return x
		},
		"tuple port range": func(x []any) []any {
			environmentNFTEntry(x, "set", "native_tcp")["elem"] = []any{map[string]any{"concat": []any{"10.0.0.1", "10.0.0.2", map[string]any{"range": []any{json.Number("1"), json.Number("65535")}}}}}
			return x
		},
		"changed bridge": func(x []any) []any {
			m := environmentNFTRule(x, "forward", 0)["expr"].([]any)[0].(map[string]any)["match"].(map[string]any)
			m["right"] = "br-other"
			return x
		},
		"unconditional accept": func(x []any) []any {
			environmentNFTRule(x, "boundary", 0)["expr"] = []any{map[string]any{"accept": nil}}
			return x
		},
		"established original traffic": func(x []any) []any {
			r := environmentNFTRule(x, "boundary", 1)
			r["expr"] = r["expr"].([]any)[1:]
			return x
		},
		"wrong tuple lookup": func(x []any) []any {
			m := environmentNFTRule(x, "boundary", 3)["expr"].([]any)[0].(map[string]any)["match"].(map[string]any)
			m["right"] = "@native_udp"
			return x
		},
		"global widened": func(x []any) []any {
			m := environmentNFTRule(x, "boundary", 5)["expr"].([]any)[0].(map[string]any)["match"].(map[string]any)
			m["right"] = map[string]any{"prefix": map[string]any{"addr": "203.0.0.0", "len": json.Number("16")}}
			return x
		},
		"global widened ports": func(x []any) []any {
			m := environmentNFTRule(x, "boundary", 5)["expr"].([]any)[1].(map[string]any)["match"].(map[string]any)
			m["right"] = map[string]any{"range": []any{json.Number("1"), json.Number("65535")}}
			return x
		},
		"extra accept rule": func(x []any) []any {
			r := map[string]any{"family": "inet", "table": "floci_iso_sample", "chain": "boundary", "expr": []any{map[string]any{"accept": nil}}}
			return append(x, map[string]any{"rule": r})
		},
		"missing final drop": func(x []any) []any { return x[:len(x)-1] },
		"extra expression": func(x []any) []any {
			r := environmentNFTRule(x, "boundary", 7)
			r["expr"] = append(r["expr"].([]any), map[string]any{"accept": nil})
			return x
		},
		"negative counter": func(x []any) []any {
			environmentNFTRule(x, "boundary", 7)["expr"].([]any)[0].(map[string]any)["counter"].(map[string]any)["bytes"] = json.Number("-1")
			return x
		},
		"unexpected map": func(x []any) []any {
			return append(x, map[string]any{"map": map[string]any{"family": "inet", "table": "floci_iso_sample", "name": "escape"}})
		},
		"duplicate object": func(x []any) []any { return append(x, map[string]any{"table": environmentNFTEntry(x, "table", "")}) },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			data := mutateEnvironmentNFT(t, change)
			if verifyEnvironmentNFT(data, "floci_iso_sample", []string{"br-one", "br-two"}, environmentNFTGlobals()) == nil {
				t.Fatal("changed enforcement accepted")
			}
		})
	}
}

func TestEnvironmentNFTAllowsOnlyHarmlessSerializationDifferences(t *testing.T) {
	data := mutateEnvironmentNFT(t, func(x []any) []any {
		for _, o := range x {
			for kind, value := range o.(map[string]any) {
				entry := value.(map[string]any)
				if kind != "metainfo" {
					entry["handle"] = json.Number("18446744073709551615")
				}
				if kind == "set" {
					entry["flags"] = []any{}
				}
				if kind == "rule" {
					for _, value := range entry["expr"].([]any) {
						expr := value.(map[string]any)
						if counter, ok := expr["counter"].(map[string]any); ok {
							counter["packets"] = json.Number("18446744073709551615")
							counter["bytes"] = json.Number("400")
						}
						if match, ok := expr["match"].(map[string]any); ok {
							left := match["left"].(map[string]any)
							if meta, ok := left["meta"].(map[string]any); ok && (meta["key"] == "iifname" || meta["key"] == "oifname") {
								match["right"] = map[string]any{"set": []any{"br-one"}}
							}
						}
					}
				}
			}
		}
		return x
	})
	if err := verifyEnvironmentNFT(data, "floci_iso_sample", []string{"br-one"}, environmentNFTGlobals()); err != nil {
		t.Fatal(err)
	}
}
