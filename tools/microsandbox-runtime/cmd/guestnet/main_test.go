package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDNSRoutingRequiresExplicitStandaloneMode(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		args []string
		want string
	}{
		{"without mode", `{"dnsRouting":{"nameservers":["10.1.2.3:5300"]}}`, nil, "requires --configure-dns"},
		{"missing routing", `{}`, []string{"--configure-dns"}, "requires a nameservers"},
		{"ready file", `{"dnsRouting":{"nameservers":["10.1.2.3:5300"]}}`, []string{"--configure-dns", "--ready-file", "unused"}, "ready-file"},
		{"listeners", `{"dnsRouting":{"nameservers":["10.1.2.3:5300"]},"http":[{}]}`, []string{"--configure-dns"}, "invalid guest"},
		{"loopback", `{"dnsRouting":{"nameservers":["10.1.2.3:5300"]},"loopbackAddresses":["169.254.169.254"]}`, []string{"--configure-dns"}, "invalid guest"},
		{"second config", `{} {}`, []string{"--configure-dns"}, "exactly one"},
		{"unknown option", `{"dnsRouting":{"listen":"127.0.0.1:53"}}`, []string{"--configure-dns"}, "invalid guest"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(test.body), 0600); err != nil {
				t.Fatal(err)
			}
			args := append([]string{"--config", path}, test.args...)
			if err := run(t.Context(), args); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want %s", err, test.want)
			}
		})
	}
	if err := run(t.Context(), []string{"--configure-dns", "--forward", "127.0.0.1:53"}); err == nil {
		t.Fatal("routing and forwarding modes must be mutually exclusive")
	}
}
