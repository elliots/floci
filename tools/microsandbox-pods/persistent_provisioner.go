package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"time"

	native "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	localProvisionerName        = "microsandbox.dev/local"
	localVolumeFinalizer        = "microsandbox.dev/durable-volume"
	localVolumeIntentAnnotation = "microsandbox.dev/provisioning-id"
	selectedVolumeNode          = "volume.kubernetes.io/selected-node"
	provisionedVolumeBy         = "pv.kubernetes.io/provisioned-by"
	localVolumeRoot             = "/var/lib/floci/persistent-volumes/"
)

type localDiskRuntime interface {
	CreateNamedDisk(context.Context, native.NamedDiskRequest) (native.NamedDiskIdentity, error)
	InspectNamedDisk(context.Context, string) (native.NamedDiskIdentity, error)
	RemoveNamedDisk(context.Context, native.NamedDiskIdentity) error
}

type persistentVolumeIntent struct {
	Version     int                                  `json:"version"`
	Cluster     string                               `json:"cluster"`
	Node        string                               `json:"node"`
	Namespace   string                               `json:"namespace"`
	ClaimName   string                               `json:"claimName"`
	ClaimUID    string                               `json:"claimUID"`
	Class       string                               `json:"class"`
	ClassUID    string                               `json:"classUID"`
	PVName      string                               `json:"pvName"`
	Nonce       string                               `json:"nonce"`
	PVUID       types.UID                            `json:"pvUID,omitempty"`
	CapacityMiB uint32                               `json:"capacityMiB"`
	Reclaim     corev1.PersistentVolumeReclaimPolicy `json:"reclaim"`
	Phase       string                               `json:"phase"`
	Disk        *native.NamedDiskIdentity            `json:"disk,omitempty"`
}

type persistentProvisioner struct {
	client                   kubernetes.Interface
	runtime                  localDiskRuntime
	directory, node, cluster string
	mu                       sync.Mutex
	intents                  map[string]*persistentVolumeIntent
	reported                 map[types.UID]string
}

func newPersistentProvisioner(client kubernetes.Interface, runtime localDiskRuntime, stateDir, nodeName, clusterUID string) (*persistentProvisioner, error) {
	if client == nil || runtime == nil || stateDir == "" || nodeName == "" || clusterUID == "" {
		return nil, errors.New("persistent provisioner requires a client, runtime, state and node/cluster identities")
	}
	p := &persistentProvisioner{client: client, runtime: runtime, directory: filepath.Join(stateDir, "persistent-volumes"), node: nodeName, cluster: clusterUID, intents: map[string]*persistentVolumeIntent{}, reported: map[types.UID]string{}}
	if err := os.MkdirAll(p.directory, 0700); err != nil {
		return nil, err
	}
	paths, err := filepath.Glob(filepath.Join(p.directory, "*.json"))
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var record persistentVolumeIntent
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, err
		}
		if record.Version != 1 || record.Cluster != clusterUID || record.Node != nodeName || record.ClaimUID == "" || record.Namespace == "" || record.ClaimName == "" || record.Class == "" || record.ClassUID == "" || record.Nonce == "" || record.CapacityMiB < 256 ||
			record.PVName != persistentVolumeName(clusterUID, record.ClaimUID) || filepath.Base(path) != record.PVName+".json" ||
			!slices.Contains([]string{"creating", "publishing", "ready", "deleting", "retained", "deleted"}, record.Phase) ||
			!slices.Contains([]corev1.PersistentVolumeReclaimPolicy{corev1.PersistentVolumeReclaimDelete, corev1.PersistentVolumeReclaimRetain}, record.Reclaim) {
			return nil, fmt.Errorf("invalid persistent volume ownership intent %s", filepath.Base(path))
		}
		if record.Disk != nil && !p.validDisk(&record, *record.Disk) {
			return nil, errors.New("persistent volume intent has invalid disk ownership")
		}
		if record.Phase != "creating" && record.Disk == nil {
			return nil, errors.New("persistent volume intent lost its disk identity")
		}
		p.intents[record.PVName] = &record
	}
	return p, nil
}

func persistentVolumeName(cluster, uid string) string {
	digest := sha256.Sum256([]byte(cluster + "\x00" + uid))
	return "floci-pvc-" + hex.EncodeToString(digest[:20])
}

func (p *persistentProvisioner) diskRequest(record *persistentVolumeIntent) native.NamedDiskRequest {
	return native.NamedDiskRequest{Name: record.PVName, CapacityMiB: record.CapacityMiB, Labels: map[string]string{
		"floci.cluster.uid": p.cluster, "floci.node": p.node, "floci.pvc.uid": record.ClaimUID, "floci.pv.intent": record.Nonce,
	}}
}

func (p *persistentProvisioner) validDisk(record *persistentVolumeIntent, actual native.NamedDiskIdentity) bool {
	wanted := p.diskRequest(record)
	return actual.Name == wanted.Name && actual.CapacityMiB == wanted.CapacityMiB && actual.Path != "" && maps.Equal(actual.Labels, wanted.Labels)
}

func (p *persistentProvisioner) save(record *persistentVolumeIntent) error {
	file, err := os.CreateTemp(p.directory, ".intent-")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	if err := json.NewEncoder(file).Encode(record); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, filepath.Join(p.directory, record.PVName+".json")); err != nil {
		return err
	}
	directory, err := os.Open(p.directory)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// Run uses one controller loop for all claims, including reclaim retries while
// sleeping Pods retain their native disk attachments.
func (p *persistentProvisioner) Run(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	lastError := ""
	for {
		err := p.reconcile(ctx)
		if err == nil {
			lastError = ""
		} else if err.Error() != lastError && ctx.Err() == nil {
			lastError = err.Error()
			slog.Warn("native persistent volume reconciliation", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (p *persistentProvisioner) reconcile(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	claims, err := p.client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	var failures []error
	observed := map[types.UID]bool{}
	for i := range claims.Items {
		claim := &claims.Items[i]
		observed[claim.UID] = true
		if claim.DeletionTimestamp != nil || claim.Spec.VolumeName != "" || claim.Annotations[selectedVolumeNode] != p.node || claim.Spec.StorageClassName == nil {
			continue
		}
		class, err := p.client.StorageV1().StorageClasses().Get(ctx, *claim.Spec.StorageClassName, metav1.GetOptions{})
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if class.Provisioner != localProvisionerName {
			continue
		}
		if err := p.provision(ctx, claim, class); err != nil {
			failures = append(failures, fmt.Errorf("PVC %s/%s: %w", claim.Namespace, claim.Name, err))
			p.reportFailure(ctx, claim, err)
		} else {
			delete(p.reported, claim.UID)
		}
	}
	maps.DeleteFunc(p.reported, func(uid types.UID, _ string) bool { return !observed[uid] })
	for _, name := range slices.Sorted(maps.Keys(p.intents)) {
		record := p.intents[name]
		if err := p.reconcileIntent(ctx, record); err != nil {
			failures = append(failures, fmt.Errorf("PV %s: %w", name, err))
		}
	}
	return errors.Join(failures...)
}

func (p *persistentProvisioner) reportFailure(ctx context.Context, claim *corev1.PersistentVolumeClaim, failure error) {
	if p.reported[claim.UID] == failure.Error() {
		return
	}
	event := &corev1.Event{ObjectMeta: metav1.ObjectMeta{GenerateName: claim.Name + "-", Namespace: claim.Namespace},
		InvolvedObject: corev1.ObjectReference{Kind: "PersistentVolumeClaim", APIVersion: "v1", Namespace: claim.Namespace, Name: claim.Name, UID: claim.UID},
		Type:           corev1.EventTypeWarning, Reason: "ProvisioningFailed", Message: failure.Error(), Source: corev1.EventSource{Component: localProvisionerName},
		FirstTimestamp: metav1.Now(), LastTimestamp: metav1.Now(), Count: 1}
	if _, err := p.client.CoreV1().Events(claim.Namespace).Create(ctx, event, metav1.CreateOptions{}); err != nil {
		slog.Warn("report native volume provisioning failure", "namespace", claim.Namespace, "claim", claim.Name, "error", err)
		return
	}
	p.reported[claim.UID] = failure.Error()
}

func nativeClaimCapacity(claim *corev1.PersistentVolumeClaim, class *storagev1.StorageClass, node string) (uint32, corev1.PersistentVolumeReclaimPolicy, error) {
	if claim.UID == "" || class.UID == "" || claim.Spec.Selector != nil || claim.Spec.DataSource != nil || claim.Spec.DataSourceRef != nil ||
		claim.Spec.VolumeAttributesClassName != nil || !reflect.DeepEqual(claim.Spec.AccessModes, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) ||
		claim.Spec.VolumeMode != nil && *claim.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return 0, "", errors.New("native local provisioning requires a Filesystem ReadWriteOnce claim without selectors or data sources")
	}
	if class.VolumeBindingMode == nil || *class.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer ||
		class.AllowVolumeExpansion != nil && *class.AllowVolumeExpansion || len(class.Parameters) != 0 || len(class.MountOptions) != 0 {
		return 0, "", errors.New("native local StorageClass requires WaitForFirstConsumer, no expansion, parameters or mount options")
	}
	allowed := len(class.AllowedTopologies) == 0
	for _, term := range class.AllowedTopologies {
		matches := len(term.MatchLabelExpressions) > 0
		for _, expression := range term.MatchLabelExpressions {
			if expression.Key != corev1.LabelHostname {
				return 0, "", errors.New("native local topology supports only kubernetes.io/hostname")
			}
			matches = matches && slices.Contains(expression.Values, node)
		}
		allowed = allowed || matches
	}
	if !allowed {
		return 0, "", errors.New("selected node is outside the StorageClass topology")
	}
	requested := claim.Spec.Resources.Requests[corev1.ResourceStorage]
	maximum := resource.NewQuantity(int64(math.MaxUint32)*1024*1024, resource.BinarySI)
	if requested.Sign() <= 0 || requested.Cmp(*maximum) > 0 {
		return 0, "", errors.New("native local storage request must be positive and within the supported disk range")
	}
	bytes := requested.Value()
	capacity := uint32(max(256, (bytes+1024*1024-1)/(1024*1024)))
	if limit, exists := claim.Spec.Resources.Limits[corev1.ResourceStorage]; exists && limit.Cmp(*resource.NewQuantity(int64(capacity)*1024*1024, resource.BinarySI)) < 0 {
		return 0, "", errors.New("native disk minimum or rounding exceeds the claim storage limit")
	}
	reclaim := corev1.PersistentVolumeReclaimDelete
	if class.ReclaimPolicy != nil {
		reclaim = *class.ReclaimPolicy
	}
	if reclaim != corev1.PersistentVolumeReclaimDelete && reclaim != corev1.PersistentVolumeReclaimRetain {
		return 0, "", errors.New("native local volumes support only Delete and Retain reclaim policies")
	}
	return capacity, reclaim, nil
}

func (p *persistentProvisioner) provision(ctx context.Context, claim *corev1.PersistentVolumeClaim, class *storagev1.StorageClass) error {
	capacity, reclaim, err := nativeClaimCapacity(claim, class, p.node)
	if err != nil {
		return err
	}
	name := persistentVolumeName(p.cluster, string(claim.UID))
	record := p.intents[name]
	if record == nil {
		record = &persistentVolumeIntent{Version: 1, Cluster: p.cluster, Node: p.node, Namespace: claim.Namespace, ClaimName: claim.Name,
			ClaimUID: string(claim.UID), Class: class.Name, ClassUID: string(class.UID), PVName: name, Nonce: rand.Text(), CapacityMiB: capacity, Reclaim: reclaim, Phase: "creating"}
		if err := p.save(record); err != nil {
			return err
		}
		p.intents[name] = record
	}
	if record.Namespace != claim.Namespace || record.ClaimName != claim.Name || record.Class != class.Name || record.ClassUID != string(class.UID) || record.CapacityMiB != capacity {
		return errors.New("PVC differs from its durable provisioning intent; expansion is unsupported")
	}
	if record.Phase == "deleted" || record.Phase == "deleting" || record.Phase == "retained" {
		return errors.New("PVC allocation is retired and its disk name cannot be reused")
	}
	return p.reconcileIntent(ctx, record)
}

func (p *persistentProvisioner) currentClaim(ctx context.Context, record *persistentVolumeIntent) (*corev1.PersistentVolumeClaim, bool, error) {
	claim, err := p.client.CoreV1().PersistentVolumeClaims(record.Namespace).Get(ctx, record.ClaimName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return claim, string(claim.UID) == record.ClaimUID && claim.DeletionTimestamp == nil && (claim.Spec.VolumeName == "" || claim.Spec.VolumeName == record.PVName), nil
}

func (p *persistentProvisioner) validatePendingClaim(ctx context.Context, record *persistentVolumeIntent, claim *corev1.PersistentVolumeClaim) error {
	if claim.Annotations[selectedVolumeNode] != p.node || claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != record.Class {
		return errors.New("PVC selected node or StorageClass changed during provisioning")
	}
	class, err := p.client.StorageV1().StorageClasses().Get(ctx, record.Class, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(class.UID) != record.ClassUID || class.Provisioner != localProvisionerName {
		return errors.New("StorageClass identity changed during native provisioning")
	}
	capacity, _, err := nativeClaimCapacity(claim, class, p.node)
	if err != nil {
		return err
	}
	if capacity != record.CapacityMiB {
		return errors.New("PVC size changed during native provisioning; expansion is unsupported")
	}
	return nil
}

func (p *persistentProvisioner) reconcileIntent(ctx context.Context, record *persistentVolumeIntent) error {
	if record.Phase == "retained" || record.Phase == "deleted" {
		return p.finishDeletedPV(ctx, record)
	}
	pv, err := p.client.CoreV1().PersistentVolumes().Get(ctx, record.PVName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil {
		if err := p.validatePV(record, pv); err != nil {
			return err
		}
		if record.PVUID == "" {
			if pv.Spec.ClaimRef == nil || string(pv.Spec.ClaimRef.UID) != record.ClaimUID {
				return errors.New("new PV no longer references the original claim")
			}
			record.PVUID, record.Phase = pv.UID, "ready"
			if err := p.save(record); err != nil {
				return err
			}
		}
		return p.reclaim(ctx, record, pv)
	}
	if record.PVUID != "" {
		if record.Phase == "deleting" {
			return p.deleteDisk(record, ctx)
		}
		return errors.New("recorded PV disappeared; preserving its disk instead of recreating or reclaiming it")
	}
	claim, active, err := p.currentClaim(ctx, record)
	if err != nil {
		return err
	}
	if active {
		if err := p.validatePendingClaim(ctx, record, claim); err != nil {
			return err
		}
	}
	if record.Disk == nil {
		var actual native.NamedDiskIdentity
		if active {
			if claim.Annotations[selectedVolumeNode] != p.node {
				return errors.New("selected node changed during native provisioning")
			}
			actual, err = p.runtime.CreateNamedDisk(ctx, p.diskRequest(record))
		} else {
			actual, err = p.runtime.InspectNamedDisk(ctx, record.PVName)
			if errors.Is(err, native.ErrNamedDiskNotFound) {
				return errors.New("claim disappeared during disk creation; preserving the uncertain allocation intent")
			}
		}
		if err != nil {
			return err
		}
		if !p.validDisk(record, actual) {
			return errors.New("native disk differs from its provisioning intent")
		}
		record.Disk = &actual
		if err := p.save(record); err != nil {
			return err
		}
	}
	if record.Phase == "publishing" {
		return errors.New("PV publication had an uncertain outcome and the PV is absent; preserving the disk")
	}
	if !active && record.Reclaim == corev1.PersistentVolumeReclaimDelete {
		return p.deleteDisk(record, ctx)
	}
	// Recheck the exact claim before publication. An old UID can never bind a
	// replacement claim with the same namespace/name.
	claim, active, err = p.currentClaim(ctx, record)
	if err != nil {
		return err
	}
	if active {
		if err := p.validatePendingClaim(ctx, record, claim); err != nil {
			return err
		}
	}
	if !active && record.Reclaim == corev1.PersistentVolumeReclaimDelete {
		return p.deleteDisk(record, ctx)
	}
	record.Phase = "publishing"
	if err := p.save(record); err != nil {
		return err
	}
	created, err := p.client.CoreV1().PersistentVolumes().Create(ctx, p.volume(record), metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsNotFound(err) {
			// These API rejections did not create an object. Preserve the disk,
			// but permit publication after RBAC/admission configuration is fixed.
			record.Phase = "creating"
			return errors.Join(err, p.save(record))
		}
		return err
	}
	if err := p.validatePV(record, created); err != nil {
		return err
	}
	record.PVUID, record.Phase = created.UID, "ready"
	return p.save(record)
}

func (p *persistentProvisioner) volume(record *persistentVolumeIntent) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: record.PVName, Finalizers: []string{localVolumeFinalizer}, Annotations: map[string]string{
		provisionedVolumeBy: localProvisionerName, localVolumeIntentAnnotation: record.Nonce,
	}}, Spec: corev1.PersistentVolumeSpec{
		Capacity:               corev1.ResourceList{corev1.ResourceStorage: *resource.NewQuantity(int64(record.CapacityMiB)*1024*1024, resource.BinarySI)},
		PersistentVolumeSource: corev1.PersistentVolumeSource{Local: &corev1.LocalVolumeSource{Path: localVolumeRoot + record.PVName, FSType: new("ext4")}},
		AccessModes:            []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, VolumeMode: new(corev1.PersistentVolumeFilesystem),
		StorageClassName: record.Class, PersistentVolumeReclaimPolicy: record.Reclaim,
		ClaimRef:     &corev1.ObjectReference{Kind: "PersistentVolumeClaim", APIVersion: "v1", Namespace: record.Namespace, Name: record.ClaimName, UID: types.UID(record.ClaimUID)},
		NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{p.node}}}}}}},
	}}
}

func (p *persistentProvisioner) validatePV(record *persistentVolumeIntent, pv *corev1.PersistentVolume) error {
	wanted := p.volume(record)
	actualSize, expectedSize := pv.Spec.Capacity[corev1.ResourceStorage], wanted.Spec.Capacity[corev1.ResourceStorage]
	if pv.UID == "" || record.PVUID != "" && pv.UID != record.PVUID || pv.Name != record.PVName || record.Disk == nil ||
		pv.Annotations[provisionedVolumeBy] != localProvisionerName || pv.Annotations[localVolumeIntentAnnotation] != record.Nonce ||
		pv.Spec.StorageClassName != record.Class || actualSize.Cmp(expectedSize) != 0 ||
		!reflect.DeepEqual(pv.Spec.PersistentVolumeSource, wanted.Spec.PersistentVolumeSource) ||
		!reflect.DeepEqual(pv.Spec.AccessModes, wanted.Spec.AccessModes) || !reflect.DeepEqual(pv.Spec.NodeAffinity, wanted.Spec.NodeAffinity) ||
		pv.Spec.VolumeMode != nil && *pv.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return errors.New("PV identity, native disk geometry, source or node affinity differs from its durable intent")
	}
	return nil
}

func (p *persistentProvisioner) reclaim(ctx context.Context, record *persistentVolumeIntent, pv *corev1.PersistentVolume) error {
	if record.Phase == "deleting" {
		return p.deleteDisk(record, ctx)
	}
	policy := pv.Spec.PersistentVolumeReclaimPolicy
	if policy != corev1.PersistentVolumeReclaimDelete && policy != corev1.PersistentVolumeReclaimRetain {
		return errors.New("unsupported native PV reclaim policy")
	}
	if record.Reclaim != policy {
		record.Reclaim = policy
		if err := p.save(record); err != nil {
			return err
		}
	}
	if pv.DeletionTimestamp == nil && pv.Status.Phase != corev1.VolumeReleased {
		return nil
	}
	if policy == corev1.PersistentVolumeReclaimRetain {
		if pv.DeletionTimestamp == nil {
			return nil
		}
		record.Phase = "retained"
		if err := p.save(record); err != nil {
			return err
		}
		return p.finishDeletedPV(ctx, record)
	}
	if ref := pv.Spec.ClaimRef; ref != nil {
		claim, err := p.client.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil && claim.UID == ref.UID {
			return nil
		}
	}
	if pv.DeletionTimestamp == nil {
		return p.client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pv.UID, ResourceVersion: &pv.ResourceVersion}})
	}
	record.Phase = "deleting"
	if err := p.save(record); err != nil {
		return err
	}
	return p.deleteDisk(record, ctx)
}

func (p *persistentProvisioner) deleteDisk(record *persistentVolumeIntent, ctx context.Context) error {
	if record.Disk == nil {
		return errors.New("cannot reclaim an unrecorded native disk")
	}
	record.Phase = "deleting"
	if err := p.save(record); err != nil {
		return err
	}
	if err := p.runtime.RemoveNamedDisk(ctx, *record.Disk); err != nil {
		return err
	}
	record.Phase = "deleted"
	if err := p.save(record); err != nil {
		return err
	}
	return p.finishDeletedPV(ctx, record)
}

func (p *persistentProvisioner) finishDeletedPV(ctx context.Context, record *persistentVolumeIntent) error {
	pv, err := p.client.CoreV1().PersistentVolumes().Get(ctx, record.PVName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := p.validatePV(record, pv); err != nil {
		return err
	}
	if pv.DeletionTimestamp == nil {
		return errors.New("retired native PV still exists without a deletion timestamp")
	}
	if !slices.Contains(pv.Finalizers, localVolumeFinalizer) {
		return nil
	}
	pv.Finalizers = slices.DeleteFunc(pv.Finalizers, func(value string) bool { return value == localVolumeFinalizer })
	_, err = p.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	return err
}

// Resolve is read-only: a missing disk or changed PV must never become an empty
// replacement merely because a Pod starts or wakes.
func (p *persistentProvisioner) Resolve(ctx context.Context, namespace, claimName string) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, native.NamedDiskIdentity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var empty native.NamedDiskIdentity
	claim, err := p.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, empty, err
	}
	if claim.UID == "" || claim.DeletionTimestamp != nil || claim.Status.Phase != corev1.ClaimBound || claim.Spec.VolumeName == "" {
		return nil, nil, empty, errors.New("persistent claim is not bound and available")
	}
	record := p.intents[claim.Spec.VolumeName]
	if record == nil || record.PVUID == "" || record.Phase != "ready" || record.Disk == nil {
		return nil, nil, empty, errors.New("bound PV has no active native ownership intent")
	}
	pv, err := p.client.CoreV1().PersistentVolumes().Get(ctx, record.PVName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, empty, err
	}
	if err := p.validatePV(record, pv); err != nil {
		return nil, nil, empty, err
	}
	ref := pv.Spec.ClaimRef
	request := claim.Spec.Resources.Requests[corev1.ResourceStorage]
	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	if pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound || ref == nil || ref.UID != claim.UID || ref.Namespace != namespace || ref.Name != claimName ||
		claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != record.Class || request.Sign() <= 0 || request.Cmp(capacity) > 0 ||
		!reflect.DeepEqual(claim.Spec.AccessModes, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) ||
		claim.Spec.VolumeMode != nil && *claim.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return nil, nil, empty, errors.New("PVC/PV binding, node placement or requested geometry is invalid; expansion is unsupported")
	}
	actual, err := p.runtime.InspectNamedDisk(ctx, record.Disk.Name)
	if err != nil {
		return nil, nil, empty, err
	}
	if !record.Disk.Matches(actual) {
		return nil, nil, empty, errors.New("native persistent disk identity changed")
	}
	return claim, pv, actual, nil
}
