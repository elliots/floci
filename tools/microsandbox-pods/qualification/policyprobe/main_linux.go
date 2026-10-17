// policyprobe is a test workload, never a controller or guest helper.
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

func main() {
	if len(os.Args) == 3 && os.Args[1] == "egress6" {
		address := net.ParseIP(os.Args[2])
		if address == nil || address.To4() != nil {
			fmt.Fprintln(os.Stderr, "IPv6 fixture requires an explicit IPv6 destination")
			os.Exit(1)
		}
		fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM, 0)
		if err == nil {
			err = unix.Sendto(fd, []byte("isolated"), 0, &unix.SockaddrInet6{Addr: [16]byte(address), Port: 45679})
			unix.Close(fd)
		}
		if !errors.Is(err, unix.EPERM) {
			fmt.Fprintln(os.Stderr, "IPv6 direct egress was not denied:", err)
			os.Exit(1)
		}
		fmt.Println("IPv6 direct egress denied")
		return
	}
	if len(os.Args) == 4 && os.Args[1] == "spoof" {
		if err := spoofNativeSource(os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 4 && os.Args[1] == "send" {
		if err := sendVXLAN(os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := check(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("ordinary application sockets cannot forge node marks or raw packets")
}

func spoofNativeSource(source, destination string) error {
	src, dst := net.ParseIP(source).To4(), net.ParseIP(destination).To4()
	if src == nil || dst == nil {
		return errors.New("spoof fixture requires IPv4 addresses")
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_RAW, unix.IPPROTO_RAW)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	packet := make([]byte, 28)
	packet[0], packet[8], packet[9] = 0x45, 64, unix.IPPROTO_UDP
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(packet)))
	copy(packet[12:16], src)
	copy(packet[16:20], dst)
	var sum uint32
	for i := 0; i < 20; i += 2 {
		sum += uint32(binary.BigEndian.Uint16(packet[i : i+2]))
	}
	for sum>>16 != 0 {
		sum = sum&0xffff + sum>>16
	}
	binary.BigEndian.PutUint16(packet[10:12], ^uint16(sum))
	binary.BigEndian.PutUint16(packet[20:22], 45679)
	binary.BigEndian.PutUint16(packet[22:24], 45679)
	binary.BigEndian.PutUint16(packet[24:26], 8)
	return unix.Sendto(fd, packet, 0, &unix.SockaddrInet4{Addr: [4]byte(dst)})
}

func sendVXLAN(host, port string) error {
	connection, err := net.Dial("udp4", net.JoinHostPort(host, port))
	if err != nil {
		return err
	}
	defer connection.Close()
	// A valid VXLAN header with an Ethernet payload exercises the outer-port
	// boundary before the peer can trust the supplied VNI or inner source.
	packet := make([]byte, 64)
	packet[0], packet[5], packet[6] = 8, 0x27, 0x10
	packet[20], packet[21], packet[22] = 8, 0, 0x45
	_, err = connection.Write(packet)
	return err
}

func check() error {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	mark, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_MARK)
	if err != nil {
		return err
	}
	if mark != 0 {
		return fmt.Errorf("application inherited node mark: %x", mark)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_MARK, 0x40000000); !errors.Is(err, unix.EPERM) {
		return fmt.Errorf("SO_MARK escape: %v", err)
	}
	raw, err := unix.Socket(unix.AF_PACKET, unix.SOCK_RAW, 0)
	if err == nil {
		unix.Close(raw)
		return errors.New("raw packet socket escaped capability admission")
	}
	if !errors.Is(err, unix.EPERM) {
		return fmt.Errorf("unexpected raw socket error: %w", err)
	}
	group, err := os.OpenFile("/sys/fs/cgroup/cgroup.procs", os.O_WRONLY, 0)
	if err == nil {
		group.Close()
		return errors.New("application can write the cgroup hierarchy")
	}
	if !errors.Is(err, unix.EROFS) && !errors.Is(err, unix.EACCES) && !errors.Is(err, unix.EPERM) {
		return fmt.Errorf("unexpected cgroup mount error: %w", err)
	}
	for _, value := range os.Args[1:] {
		port, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		address := &unix.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}}
		if err := unix.Connect(fd, address); !errors.Is(err, unix.EPERM) {
			return fmt.Errorf("connected UDP transport escape on %d: %v", port, err)
		}
		if err := unix.Sendto(fd, make([]byte, 64), 0, address); !errors.Is(err, unix.EPERM) {
			return fmt.Errorf("unconnected UDP transport escape on %d: %v", port, err)
		}
		mapped, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM, 0)
		if err != nil {
			return err
		}
		address6 := &unix.SockaddrInet6{Port: port, Addr: [16]byte{10: 0xff, 11: 0xff, 12: 127, 15: 1}}
		connected := unix.Connect(mapped, address6)
		sent := unix.Sendto(mapped, make([]byte, 64), 0, address6)
		unix.Close(mapped)
		if !errors.Is(connected, unix.EPERM) || !errors.Is(sent, unix.EPERM) {
			return fmt.Errorf("IPv4-mapped UDP transport escape on %d: connect=%v send=%v", port, connected, sent)
		}
	}
	return nil
}
