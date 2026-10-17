package policyguard

import (
	"errors"
	"fmt"
	"os"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/asm"
	"github.com/cilium/ebpf/link"
	"golang.org/x/sys/unix"
)

func nodeProgram() *ebpf.ProgramSpec {
	return &ebpf.ProgramSpec{Name: "floci_node_v1", Type: ebpf.CGroupSock,
		AttachType: ebpf.AttachCGroupInetSockCreate, License: "GPL",
		Instructions: asm.Instructions{
			// struct bpf_sock.mark is the fifth u32 in the stable Linux UAPI.
			asm.StoreImm(asm.R1, 16, NodeMark, asm.Word),
			asm.Mov.Imm(asm.R0, 1), asm.Return(),
		}}
}

func guardProgram() *ebpf.ProgramSpec {
	otherLevel := asm.JNE.Imm(asm.R2, unix.SOL_SOCKET, "")
	otherLevel.Offset = 3
	otherOption := asm.JNE.Imm(asm.R2, unix.SO_MARK, "")
	otherOption.Offset = 1
	return &ebpf.ProgramSpec{Name: "floci_mark_v1", Type: ebpf.CGroupSockopt,
		AttachType: ebpf.AttachCGroupSetsockopt, License: "GPL",
		Instructions: asm.Instructions{
			asm.Mov.Imm(asm.R0, 1),
			// bpf_sockopt has three u64 pointers followed by level and optname.
			asm.LoadMem(asm.R2, asm.R1, 24, asm.Word),
			otherLevel,
			asm.LoadMem(asm.R2, asm.R1, 28, asm.Word),
			otherOption,
			asm.Mov.Imm(asm.R0, 0),
			asm.Return(),
		}}
}

// InstallNode must run before containerd makes any registry connections.
// Application containers use separate root cgroups, so they cannot inherit it.
func InstallNode() error {
	return install("microsandbox-workload", nodeProgram())
}

// InstallApplication prevents changing socket marks in policy-compatible Pods.
// Raw packet and administration capabilities are rejected separately by admission.
func InstallApplication(name string, transportPort int) error {
	if err := validateApplication(name); err != nil {
		return err
	}
	if transportPort < 1 || transportPort > 65535 {
		return errors.New("policy guard requires a valid owned transport port")
	}
	if err := install(name, guardProgram()); err != nil {
		return err
	}
	for _, hook := range []struct {
		name   string
		attach ebpf.AttachType
	}{{"floci_udp4c_v1", ebpf.AttachCGroupInet4Connect}, {"floci_udp6c_v1", ebpf.AttachCGroupInet6Connect},
		{"floci_udp4s_v1", ebpf.AttachCGroupUDP4Sendmsg}, {"floci_udp6s_v1", ebpf.AttachCGroupUDP6Sendmsg}} {
		if err := install(name, transportProgram(hook.name, hook.attach, transportPort)); err != nil {
			return err
		}
	}
	return nil
}

func transportProgram(name string, attach ebpf.AttachType, port int) *ebpf.ProgramSpec {
	otherProtocol := asm.JNE.Imm(asm.R2, unix.IPPROTO_UDP, "")
	otherProtocol.Offset = 5
	guestTransport := asm.JEq.Imm(asm.R2, 8472, "")
	guestTransport.Offset = 1
	otherPort := asm.JNE.Imm(asm.R2, int32(port), "")
	otherPort.Offset = 1
	return &ebpf.ProgramSpec{Name: name, Type: ebpf.CGroupSockAddr, AttachType: attach, License: "GPL",
		Instructions: asm.Instructions{
			asm.Mov.Imm(asm.R0, 1),
			// struct bpf_sock_addr exposes protocol and network-order user_port.
			asm.LoadMem(asm.R2, asm.R1, 36, asm.Word), otherProtocol,
			asm.LoadMem(asm.R2, asm.R1, 24, asm.Word), asm.HostTo(asm.BE, asm.R2, asm.Half),
			guestTransport, otherPort,
			asm.Mov.Imm(asm.R0, 0), asm.Return(),
		}}
}

func install(name string, spec *ebpf.ProgramSpec) error {
	root, err := os.OpenRoot("/sys/fs/cgroup")
	if err != nil {
		return err
	}
	defer root.Close()
	info, err := root.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("policy cgroup must be a real directory")
	}
	group, err := root.Open(name)
	if err != nil {
		return err
	}
	defer group.Close()
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(group.Fd()), &stat); err != nil || stat.Type != unix.CGROUP2_SUPER_MAGIC {
		return errors.New("policy hooks require cgroup v2")
	}
	query, err := link.QueryPrograms(link.QueryOptions{Target: int(group.Fd()), Attach: spec.AttachType})
	if err != nil {
		return fmt.Errorf("query policy socket hook: %w", err)
	}
	if len(query.Programs) != 0 {
		if len(query.Programs) != 1 {
			return errors.New("policy cgroup has unexpected socket hooks")
		}
		program, err := ebpf.NewProgramFromID(query.Programs[0].ID)
		if err != nil {
			return err
		}
		defer program.Close()
		actual, err := program.Info()
		if err != nil {
			return err
		}
		if err := spec.Compatible(actual); err != nil || actual.Name != spec.Name || actual.Type != spec.Type {
			return fmt.Errorf("policy cgroup socket hook identity differs: name=%s type=%s: %v", actual.Name, actual.Type, err)
		}
		return nil
	}
	program, err := ebpf.NewProgram(spec)
	if err != nil {
		return fmt.Errorf("load policy socket hook: %w", err)
	}
	defer program.Close()
	// PROG_ATTACH retains the program in the cgroup after this process exits.
	// No pinned filesystem object, resident daemon or userspace map is required.
	return link.RawAttachProgram(link.RawAttachProgramOptions{Target: int(group.Fd()), Program: program, Attach: spec.AttachType})
}
