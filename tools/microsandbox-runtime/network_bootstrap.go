package microsandboxruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"runtime"
	"time"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

func readGuestNetworkAgent(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("native isolation requires FLOCI_MICROSANDBOX_GUEST_AGENT_PATH pointing to the Linux guestnet binary")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 16<<20+1))
	if err != nil || len(data) < 64 || len(data) > 16<<20 || !bytes.Equal(data[:4], []byte{127, 'E', 'L', 'F'}) || data[4] != 2 || data[5] != 1 {
		return nil, errors.New("native network guest agent must be a Linux ELF64 binary of at most 16 MiB")
	}
	machine := binary.LittleEndian.Uint16(data[18:20])
	if runtime.GOARCH != "arm64" && runtime.GOARCH != "amd64" || runtime.GOARCH == "arm64" && machine != 183 || runtime.GOARCH == "amd64" && machine != 62 {
		return nil, errors.New("native network guest agent architecture differs from the host")
	}
	return data, nil
}

func (b *nativeNetworkBoundary) guestDNSConfig() ([]byte, error) {
	servers := make([]string, 0, len(b.Nameservers))
	for _, value := range b.Nameservers {
		server, err := netip.ParseAddrPort(value)
		if err != nil {
			return nil, err
		}
		if server.Addr().IsLoopback() {
			servers = append(servers, net.JoinHostPort("host.microsandbox.internal", fmt.Sprint(server.Port())))
		} else {
			servers = append(servers, value)
		}
	}
	return json.Marshal(map[string]any{"dnsRouting": map[string]any{"nameservers": servers}})
}

func (b *nativeNetworkBoundary) dnsRules() []msb.PolicyRule {
	rules := make([]msb.PolicyRule, 0, len(b.Nameservers))
	for _, value := range b.Nameservers {
		server := netip.MustParseAddrPort(value)
		destination := server.Addr().String() + "/32"
		if server.Addr().IsLoopback() {
			destination = "host"
		}
		rules = append(rules, msb.PolicyRule{Action: msb.PolicyActionAllow, Direction: msb.PolicyDirectionEgress,
			Destination: destination, Protocols: []msb.PolicyProtocol{msb.PolicyProtocolUDP, msb.PolicyProtocolTCP}, Port: fmt.Sprint(server.Port())})
	}
	return rules
}

// Restore has no DNS-config field in SDK 0.7.8. Deny its default DNS gateway
// before RAM starts and retain only the explicitly configured DNS transports.
func (b *nativeNetworkBoundary) restorePolicy(original *msb.NetworkConfig) (*msb.NetworkConfig, error) {
	if original == nil {
		return nil, errors.New("checkpoint lacks its effective network policy")
	}
	policy, err := b.restrict(original)
	if err != nil {
		return nil, err
	}
	policy.DNS = nil
	policy.Rules[0] = msb.PolicyRule{Action: msb.PolicyActionDeny, Direction: msb.PolicyDirectionEgress,
		Destination: "host", Protocols: []msb.PolicyProtocol{msb.PolicyProtocolUDP, msb.PolicyProtocolTCP}, Port: "53"}
	policy.Rules = uniquePolicyRules(policy.Rules)
	return policy, nil
}

func (b *nativeNetworkBoundary) bootstrapInput() (io.ReadCloser, error) {
	if len(b.guestAgent) == 0 {
		return nil, errors.New("native DNS guest agent was not validated")
	}
	configuration, err := b.guestDNSConfig()
	if err != nil {
		return nil, err
	}
	// The one-shot Linux helper installs kernel DNAT rules and exits. Both
	// rules and conntrack state live in guest RAM; refresh them after restore
	// because the SDK can allocate a different host gateway.
	prefix := `set -eu
umask 077
directory=/var/lib/floci/network-dns
mkdir -p "$directory"
cd "$directory"
trap 'rm -f guestnet guestnet.pending config.json config.pending' EXIT
base64 -d > guestnet.pending <<'FLOCI_BINARY'
`
	suffix := `
FLOCI_BINARY
chmod 700 guestnet.pending
mv guestnet.pending guestnet
base64 -d > config.pending <<'FLOCI_CONFIG'
` + base64.StdEncoding.EncodeToString(configuration) + `
FLOCI_CONFIG
mv config.pending config.json
"$directory/guestnet" --configure-dns --config "$directory/config.json"
`
	reader, writer := io.Pipe()
	go func() {
		err := func() error {
			if _, err := io.WriteString(writer, prefix); err != nil {
				return err
			}
			encoder := base64.NewEncoder(base64.StdEncoding, writer)
			_, err := io.Copy(encoder, bytes.NewReader(b.guestAgent))
			if err = errors.Join(err, encoder.Close()); err != nil {
				return err
			}
			_, err = io.WriteString(writer, suffix)
			return err
		}()
		_ = writer.CloseWithError(err)
	}()
	return reader, nil
}

func (b *nativeNetworkBoundary) installGuestDNS(ctx context.Context, handle *msb.SandboxHandle) error {
	input, err := b.bootstrapInput()
	if err != nil {
		return err
	}
	defer input.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	sandbox, err := handle.Connect(ctx)
	if err != nil {
		return err
	}
	defer sandbox.Close()
	var diagnostic bytes.Buffer
	stderr := &boundedWriter{dst: &diagnostic}
	code, err := streamExec(ctx, sandbox, command{argv: []string{"sh", "-s"}, user: "0"}, input, io.Discard, stderr)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("native guest DNS bootstrap exited %d: %s", code, diagnostic.String())
	}
	return nil
}

func (b *nativeNetworkBoundary) completeBoot(ctx context.Context, name, home string, policy *msb.NetworkConfig) error {
	handle, err := msb.GetSandbox(ctx, name)
	if err != nil {
		return err
	}
	admissionErr := b.installGuestDNS(ctx, handle)
	if admissionErr == nil {
		admissionErr = b.recordGuest(handle, home, policy)
	}
	if admissionErr != nil {
		// A partially bootstrapped guest must not be adopted or snapshotted.
		return errors.Join(admissionErr, rejectGuestBoot(ctx, handle, home))
	}
	return nil
}

func validEffectivePolicy(policy *msb.NetworkConfig) bool {
	return policy != nil && policy.DefaultEgress == msb.PolicyActionDeny && (policy.DefaultIngress == msb.PolicyActionAllow || policy.DefaultIngress == msb.PolicyActionDeny)
}

func stopFailedNetworkBoot(ctx context.Context, name, ownedID, home string) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	handle, err := msb.GetSandbox(cleanup, name)
	if err != nil {
		return err
	}
	if ownedID == "" || handle.ID() != ownedID {
		return errors.New("cannot clean up failed boot: runtime identity changed")
	}
	return rejectGuestBoot(cleanup, handle, home)
}

func rejectGuestBoot(ctx context.Context, handle *msb.SandboxHandle, home string) error {
	// Invalidate any previous boot record even if stopping the guest fails.
	// A failed cold start must not remain adoptable under its earlier record.
	recordErr := os.Remove(boundaryRecordPath(home, handle.ID()))
	if errors.Is(recordErr, os.ErrNotExist) {
		recordErr = nil
	}
	if handle.Status() == msb.SandboxStatusStopped {
		return recordErr
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return errors.Join(recordErr, handle.Kill(cleanup))
}
