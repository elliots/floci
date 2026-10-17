package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type persistentWriteback struct {
	Version         int    `json:"version"`
	Phase           string `json:"phase"`
	Purpose         string `json:"purpose"`
	VM              string `json:"vm"`
	Generation      uint64 `json:"generation"`
	Attachments     string `json:"attachments"`
	Operation       string `json:"operation"`
	FreezeOperation string `json:"freezeOperation,omitempty"`
}

func persistentAttachmentHash(e *podEntry) string {
	data, _ := json.Marshal(e.persistentVolumes)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func validatePersistentWriteback(e *podEntry) error {
	proof := e.writeback
	if proof == nil {
		return nil
	}
	if len(e.persistentVolumes) == 0 || proof.Version != 1 || proof.VM != e.vm || proof.Generation > e.generation || proof.Attachments != persistentAttachmentHash(e) || len(proof.Operation) < 20 {
		return errors.New("persistent writeback proof has a different guest or attachment identity")
	}
	for _, ch := range proof.Operation {
		if (ch < 'A' || ch > 'Z') && (ch < '2' || ch > '7') {
			return errors.New("invalid persistent writeback operation")
		}
	}
	switch proof.Phase {
	case "SYNCED":
		if proof.Purpose != "checkpoint" || e.freeze == nil || e.freeze.Phase != "FROZEN" || proof.FreezeOperation != e.freeze.Operation {
			return errors.New("persistent checkpoint has no matching writer freeze")
		}
	case "SEALING", "SEALED":
		if proof.Generation != e.generation || (proof.Purpose != "delete" && proof.Purpose != "terminal") || proof.FreezeOperation != "" {
			return errors.New("invalid persistent deletion seal")
		}
	default:
		return errors.New("invalid persistent writeback phase")
	}
	return nil
}

func requirePersistentWriteback(e *podEntry) error {
	if len(e.persistentVolumes) == 0 {
		return nil
	}
	if err := validatePersistentWriteback(e); err != nil {
		return err
	}
	if e.writeback == nil || e.writeback.Phase == "SEALING" {
		return errors.New("persistent disk writeback is unproven; retaining the guest and checkpoints")
	}
	return nil
}

func (p *provider) invalidatePersistentWriteback(e *podEntry) error {
	if e.writeback == nil {
		return nil
	}
	if err := validatePersistentWriteback(e); err != nil {
		return err
	}
	if e.writeback.Phase != "SYNCED" {
		return errors.New("persistent filesystems are sealed for removal; applications cannot resume")
	}
	e.writeback = nil
	return p.journal(e)
}

func persistentWritebackCommand(e *podEntry, seal bool) string {
	args := []string{guestFS, "writeback"}
	if seal {
		args = append(args, "--seal")
	}
	args = append(args, "--")
	seen := map[string]bool{}
	for _, volume := range e.persistentVolumes {
		if !seen[volume.GuestPath] {
			args = append(args, volume.GuestPath)
			seen[volume.GuestPath] = true
		}
	}
	return joinCommand(args)
}

func (p *provider) syncPersistentVolumes(ctx context.Context, e *podEntry) error {
	if len(e.persistentVolumes) == 0 {
		return nil
	}
	if e.freeze == nil || e.freeze.Phase != "FROZEN" {
		return errors.New("persistent writeback requires frozen application writers")
	}
	if err := p.installGuestFS(ctx, e); err != nil {
		return err
	}
	if _, err := p.runner.guest(context.WithoutCancel(ctx), e.vm, persistentWritebackCommand(e, false)); err != nil {
		return fmt.Errorf("persistent disk writeback failed: %w", err)
	}
	e.writeback = &persistentWriteback{Version: 1, Phase: "SYNCED", Purpose: "checkpoint", VM: e.vm, Generation: e.generation, Attachments: persistentAttachmentHash(e), Operation: rand.Text(), FreezeOperation: e.freeze.Operation}
	return p.journal(e)
}

func (proof *persistentWriteback) marker() string {
	return "/run/floci-storage/" + proof.Operation + ".sealed"
}

func (p *provider) sealPersistentVolumes(ctx context.Context, e *podEntry, purpose string) error {
	if len(e.persistentVolumes) == 0 {
		return nil
	}
	if err := validatePersistentWriteback(e); err != nil {
		return err
	}
	if e.writeback != nil && e.writeback.Phase == "SEALED" {
		return nil
	}
	if e.writeback == nil || e.writeback.Phase == "SYNCED" {
		if err := p.installGuestFS(ctx, e); err != nil {
			return err
		}
		e.writeback = &persistentWriteback{Version: 1, Phase: "SEALING", Purpose: purpose, VM: e.vm, Generation: e.generation, Attachments: persistentAttachmentHash(e), Operation: rand.Text()}
		if err := p.journal(e); err != nil {
			return err
		}
		marker := quote(e.writeback.marker())
		completion := "code=$?; trap - EXIT; printf '%s\\n' \"$code\" >" + marker + ".pending && mv " + marker + ".pending " + marker + "; exit \"$code\""
		script := "umask 077; mkdir -p /run/floci-storage; test ! -e " + marker + "; trap " + quote(completion) + " EXIT; " + persistentWritebackCommand(e, true)
		if _, err := p.runner.guest(context.WithoutCancel(ctx), e.vm, script); err != nil {
			return fmt.Errorf("persistent seal outcome requires reconciliation: %w", err)
		}
	}
	// A lost response is reconciled through the original completion marker.
	// Never replay FIFREEZE or accept EBUSY as evidence of completed writeback.
	output, err := p.runner.guest(ctx, e.vm, "cat "+quote(e.writeback.marker()))
	if err != nil {
		return errors.New("persistent seal has no completion proof; retaining guest and checkpoints")
	}
	code, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil || code != 0 {
		return errors.New("persistent seal did not complete successfully; retaining guest and checkpoints")
	}
	e.writeback.Phase = "SEALED"
	return p.journal(e)
}
