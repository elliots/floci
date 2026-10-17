package main

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"testing"
	"time"

	native "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

type provisionerDisks struct {
	disks                map[string]native.NamedDiskIdentity
	creates, removes     int
	attached, lostCreate bool
	onCreate             func()
}

func (d *provisionerDisks) CreateNamedDisk(_ context.Context, request native.NamedDiskRequest) (native.NamedDiskIdentity, error) {
	if value, ok := d.disks[request.Name]; ok {
		return value, nil
	}
	d.creates++
	value := native.NamedDiskIdentity{Name: request.Name, Path: filepath.Join("/native", request.Name+".ext4"), CapacityMiB: request.CapacityMiB, Labels: maps.Clone(request.Labels), CreatedAt: time.Now(), Device: 1, Inode: uint64(d.creates)}
	d.disks[request.Name] = value
	if d.onCreate != nil {
		d.onCreate()
	}
	if d.lostCreate {
		d.lostCreate = false
		return native.NamedDiskIdentity{}, native.ErrNamedDiskUncertain
	}
	return value, nil
}
func (d *provisionerDisks) InspectNamedDisk(_ context.Context, name string) (native.NamedDiskIdentity, error) {
	if value, ok := d.disks[name]; ok {
		return value, nil
	}
	return native.NamedDiskIdentity{}, native.ErrNamedDiskNotFound
}
func (d *provisionerDisks) RemoveNamedDisk(_ context.Context, identity native.NamedDiskIdentity) error {
	if d.attached {
		return native.ErrNamedDiskAttached
	}
	value, exists := d.disks[identity.Name]
	if !exists {
		return nil
	}
	if !identity.Matches(value) {
		return errors.New("replacement disk")
	}
	d.removes++
	delete(d.disks, identity.Name)
	return nil
}

type provisionerFixture struct {
	p      *persistentProvisioner
	client *fake.Clientset
	disks  *provisionerDisks
	claim  *corev1.PersistentVolumeClaim
	class  *storagev1.StorageClass
	state  string
}

var provisionerPVResource = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}

func newProvisionerFixture(t *testing.T) *provisionerFixture {
	t.Helper()
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "apps", UID: "claim-original", Annotations: map[string]string{selectedVolumeNode: "native"}}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: new("floci-native"), AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("257Mi")}}}}
	class := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "floci-native", UID: "class-uid"}, Provisioner: localProvisionerName, VolumeBindingMode: new(storagev1.VolumeBindingWaitForFirstConsumer), ReclaimPolicy: new(corev1.PersistentVolumeReclaimDelete)}
	client := fake.NewClientset(claim, class)
	client.PrependReactor("create", "persistentvolumes", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pv := action.(clienttesting.CreateAction).GetObject().(*corev1.PersistentVolume).DeepCopy()
		pv.UID, pv.ResourceVersion = "pv-original", "1"
		return true, pv, client.Tracker().Create(provisionerPVResource, pv, "")
	})
	client.PrependReactor("delete", "persistentvolumes", func(action clienttesting.Action) (bool, runtime.Object, error) {
		value, err := client.Tracker().Get(provisionerPVResource, "", action.(clienttesting.DeleteAction).GetName())
		if err != nil {
			return true, nil, err
		}
		pv := value.(*corev1.PersistentVolume).DeepCopy()
		pv.DeletionTimestamp = new(metav1.Now())
		return true, nil, client.Tracker().Update(provisionerPVResource, pv, "")
	})
	disks := &provisionerDisks{disks: map[string]native.NamedDiskIdentity{}}
	state := t.TempDir()
	p, err := newPersistentProvisioner(client, disks, state, "native", "cluster")
	if err != nil {
		t.Fatal(err)
	}
	return &provisionerFixture{p: p, client: client, disks: disks, claim: claim, class: class, state: state}
}
func (f *provisionerFixture) provision(t *testing.T) *corev1.PersistentVolume {
	t.Helper()
	if err := f.p.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	pv, err := f.client.CoreV1().PersistentVolumes().Get(t.Context(), persistentVolumeName("cluster", string(f.claim.UID)), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return pv
}
func (f *provisionerFixture) putPV(t *testing.T, pv *corev1.PersistentVolume) {
	t.Helper()
	if _, err := f.client.CoreV1().PersistentVolumes().Update(t.Context(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}
func (f *provisionerFixture) putClaim(t *testing.T, claim *corev1.PersistentVolumeClaim) {
	t.Helper()
	if _, err := f.client.CoreV1().PersistentVolumeClaims(claim.Namespace).Update(t.Context(), claim, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}
func (f *provisionerFixture) bind(t *testing.T, pv *corev1.PersistentVolume) {
	t.Helper()
	claim, err := f.client.CoreV1().PersistentVolumeClaims("apps").Get(t.Context(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	claim.Spec.VolumeName, claim.Status.Phase = pv.Name, corev1.ClaimBound
	f.putClaim(t, claim)
	pv.Status.Phase = corev1.VolumeBound
	f.putPV(t, pv)
}
func (f *provisionerFixture) reopen(t *testing.T) {
	t.Helper()
	p, err := newPersistentProvisioner(f.client, f.disks, f.state, "native", "cluster")
	if err != nil {
		t.Fatal(err)
	}
	f.p = p
}
func (f *provisionerFixture) removeClaim(t *testing.T) {
	t.Helper()
	if err := f.client.CoreV1().PersistentVolumeClaims("apps").Delete(t.Context(), "data", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestPersistentProvisionerWaitsForSelectedNodeAndPersistsBoundIdentity(t *testing.T) {
	f := newProvisionerFixture(t)
	claim := f.claim.DeepCopy()
	delete(claim.Annotations, selectedVolumeNode)
	f.putClaim(t, claim)
	if err := f.p.reconcile(t.Context()); err != nil || f.disks.creates != 0 {
		t.Fatalf("unselected claim allocated: %v", err)
	}
	claim.Annotations[selectedVolumeNode] = "native"
	f.putClaim(t, claim)
	pv := f.provision(t)
	capacity := pv.Spec.Capacity[corev1.ResourceStorage]
	if pv.Spec.ClaimRef.UID != claim.UID || pv.Spec.Local.Path != localVolumeRoot+pv.Name || capacity.Cmp(resource.MustParse("257Mi")) != 0 {
		t.Fatalf("incorrect PV: %#v", pv.Spec)
	}
	if _, _, _, err := f.p.Resolve(t.Context(), "apps", "data"); err == nil {
		t.Fatal("unbound claim admitted")
	}
	f.bind(t, pv)
	f.reopen(t)
	_, actualPV, disk, err := f.p.Resolve(t.Context(), "apps", "data")
	if err != nil || actualPV.UID != pv.UID || disk.CapacityMiB != 257 {
		t.Fatalf("recovered binding: %v", err)
	}
	if f.disks.creates != 1 {
		t.Fatal("recovery recreated disk")
	}
}
func TestPersistentProvisionerAdmissionRejectsUnsupportedFeatures(t *testing.T) {
	cases := map[string]func(*corev1.PersistentVolumeClaim, *storagev1.StorageClass){
		"selector": func(c *corev1.PersistentVolumeClaim, _ *storagev1.StorageClass) {
			c.Spec.Selector = &metav1.LabelSelector{}
		},
		"clone": func(c *corev1.PersistentVolumeClaim, _ *storagev1.StorageClass) {
			c.Spec.DataSource = &corev1.TypedLocalObjectReference{Kind: "PersistentVolumeClaim", Name: "old"}
		},
		"rwop": func(c *corev1.PersistentVolumeClaim, _ *storagev1.StorageClass) {
			c.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
		},
		"block": func(c *corev1.PersistentVolumeClaim, _ *storagev1.StorageClass) {
			c.Spec.VolumeMode = new(corev1.PersistentVolumeBlock)
		},
		"immediate": func(_ *corev1.PersistentVolumeClaim, s *storagev1.StorageClass) {
			s.VolumeBindingMode = new(storagev1.VolumeBindingImmediate)
		},
		"expansion": func(_ *corev1.PersistentVolumeClaim, s *storagev1.StorageClass) { s.AllowVolumeExpansion = new(true) },
		"parameters": func(_ *corev1.PersistentVolumeClaim, s *storagev1.StorageClass) {
			s.Parameters = map[string]string{"type": "gp3"}
		},
		"mount options": func(_ *corev1.PersistentVolumeClaim, s *storagev1.StorageClass) { s.MountOptions = []string{"noatime"} },
		"recycle": func(_ *corev1.PersistentVolumeClaim, s *storagev1.StorageClass) {
			s.ReclaimPolicy = new(corev1.PersistentVolumeReclaimRecycle)
		},
		"zero": func(c *corev1.PersistentVolumeClaim, _ *storagev1.StorageClass) {
			c.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("0")
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newProvisionerFixture(t)
			mutate(f.claim, f.class)
			if _, _, err := nativeClaimCapacity(f.claim, f.class, "native"); err == nil {
				t.Fatal("unsupported request admitted")
			}
		})
	}
	for value, wanted := range map[string]uint32{"1": 256, "255Mi": 256, "256Mi": 256, "268435457": 257, "1.5Gi": 1536} {
		f := newProvisionerFixture(t)
		f.claim.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse(value)
		actual, _, err := nativeClaimCapacity(f.claim, f.class, "native")
		if err != nil || actual != wanted {
			t.Fatalf("size %s: %d, %v", value, actual, err)
		}
	}
}
func TestPersistentProvisionerRecoversLostDiskAndPVCreateResponses(t *testing.T) {
	for _, stage := range []string{"disk", "pv"} {
		t.Run(stage, func(t *testing.T) {
			f := newProvisionerFixture(t)
			if stage == "disk" {
				f.disks.lostCreate = true
			} else {
				f.client.PrependReactor("create", "persistentvolumes", func(action clienttesting.Action) (bool, runtime.Object, error) {
					pv := action.(clienttesting.CreateAction).GetObject().(*corev1.PersistentVolume).DeepCopy()
					pv.UID = "pv-original"
					if err := f.client.Tracker().Create(provisionerPVResource, pv, ""); err != nil {
						return true, nil, err
					}
					return true, nil, errors.New("connection lost after create")
				})
			}
			if err := f.p.provision(t.Context(), f.claim, f.class); err == nil {
				t.Fatal("missing injected failure")
			}
			f.reopen(t)
			pv := f.provision(t)
			f.bind(t, pv)
			if _, _, _, err := f.p.Resolve(t.Context(), "apps", "data"); err != nil {
				t.Fatal(err)
			}
			if f.disks.creates != 1 || f.disks.removes != 0 {
				t.Fatal("recovery replaced storage")
			}
		})
	}
}
func TestPersistentProvisionerRefusesBindingAndDiskReplacement(t *testing.T) {
	for _, change := range []string{"pv uid", "claim uid", "disk inode", "capacity", "affinity", "missing disk", "expansion"} {
		t.Run(change, func(t *testing.T) {
			f := newProvisionerFixture(t)
			pv := f.provision(t)
			f.bind(t, pv)
			switch change {
			case "pv uid":
				pv.UID = "replacement"
			case "claim uid":
				pv.Spec.ClaimRef.UID = "replacement"
			case "disk inode":
				actual := f.disks.disks[pv.Name]
				actual.Inode++
				f.disks.disks[pv.Name] = actual
			case "capacity":
				pv.Spec.Capacity[corev1.ResourceStorage] = resource.MustParse("1Gi")
			case "affinity":
				pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values = []string{"other"}
			case "missing disk":
				delete(f.disks.disks, pv.Name)
			case "expansion":
				claim, err := f.client.CoreV1().PersistentVolumeClaims("apps").Get(t.Context(), "data", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				claim.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("1Gi")
				f.putClaim(t, claim)
			}
			f.putPV(t, pv)
			if _, _, _, err := f.p.Resolve(t.Context(), "apps", "data"); err == nil {
				t.Fatal("changed binding admitted")
			}
			if f.disks.creates != 1 {
				t.Fatal("Resolve recreated storage")
			}
		})
	}
}
func TestPersistentProvisionerDeleteWaitsForAttachmentsAcrossRecovery(t *testing.T) {
	f := newProvisionerFixture(t)
	pv := f.provision(t)
	f.bind(t, pv)
	f.removeClaim(t)
	pv.Status.Phase = corev1.VolumeReleased
	f.putPV(t, pv)
	f.disks.attached = true
	if err := f.p.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.p.reconcile(t.Context()); !errors.Is(err, native.ErrNamedDiskAttached) {
		t.Fatalf("attached disk: %v", err)
	}
	if f.disks.removes != 0 {
		t.Fatal("removed attached disk")
	}
	f.reopen(t)
	f.disks.attached = false
	if err := f.p.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err := f.client.CoreV1().PersistentVolumes().Get(t.Context(), pv.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if f.disks.removes != 1 || len(after.Finalizers) != 0 {
		t.Fatal("reclamation incomplete")
	}
}
func TestPersistentProvisionerRetainAllowsSamePVRebinding(t *testing.T) {
	f := newProvisionerFixture(t)
	f.class.ReclaimPolicy = new(corev1.PersistentVolumeReclaimRetain)
	if _, err := f.client.StorageV1().StorageClasses().Update(t.Context(), f.class, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pv := f.provision(t)
	f.bind(t, pv)
	f.removeClaim(t)
	pv.Status.Phase = corev1.VolumeReleased
	f.putPV(t, pv)
	if err := f.p.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	replacement := f.claim.DeepCopy()
	replacement.UID = "claim-rebound"
	replacement.Spec.VolumeName = pv.Name
	replacement.Status.Phase = corev1.ClaimBound
	if _, err := f.client.CoreV1().PersistentVolumeClaims("apps").Create(t.Context(), replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pv.Spec.ClaimRef.UID = replacement.UID
	pv.Status.Phase = corev1.VolumeBound
	f.putPV(t, pv)
	f.reopen(t)
	if _, _, _, err := f.p.Resolve(t.Context(), "apps", "data"); err != nil {
		t.Fatal(err)
	}
	if f.disks.creates != 1 || f.disks.removes != 0 {
		t.Fatal("Retain changed backing storage")
	}
}
func TestPersistentProvisionerClaimDeletionDuringCreateDoesNotPublish(t *testing.T) {
	f := newProvisionerFixture(t)
	f.disks.onCreate = func() { f.removeClaim(t) }
	if err := f.p.provision(t.Context(), f.claim, f.class); err != nil {
		t.Fatal(err)
	}
	volumes, err := f.client.CoreV1().PersistentVolumes().List(t.Context(), metav1.ListOptions{})
	if err != nil || len(volumes.Items) != 0 || f.disks.removes != 1 {
		t.Fatalf("abandoned allocation: %v", err)
	}
}
func TestPersistentProvisionerUnknownPublicationPreservesStorage(t *testing.T) {
	f := newProvisionerFixture(t)
	f.client.PrependReactor("create", "persistentvolumes", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("unknown API outcome")
	})
	if err := f.p.provision(t.Context(), f.claim, f.class); err == nil {
		t.Fatal("missing failure")
	}
	f.removeClaim(t)
	f.reopen(t)
	if err := f.p.reconcile(t.Context()); err == nil {
		t.Fatal("unknown publication forgotten")
	}
	if f.disks.removes != 0 || len(f.disks.disks) != 1 {
		t.Fatal("uncertain publication lost data")
	}
}

func TestPersistentProvisionerRetriesDefinitivePublicationRejection(t *testing.T) {
	f := newProvisionerFixture(t)
	reject := true
	f.client.PrependReactor("create", "persistentvolumes", func(clienttesting.Action) (bool, runtime.Object, error) {
		if reject {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "persistentvolumes"}, "", errors.New("denied"))
		}
		return false, nil, nil
	})
	if err := f.p.provision(t.Context(), f.claim, f.class); !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden: %v", err)
	}
	f.reopen(t)
	reject = false
	pv := f.provision(t)
	f.bind(t, pv)
	if _, _, _, err := f.p.Resolve(t.Context(), "apps", "data"); err != nil {
		t.Fatal(err)
	}
	if f.disks.creates != 1 || f.disks.removes != 0 {
		t.Fatal("publication retry replaced data")
	}
}

func TestPersistentProvisionerFencesChangedAdmissionDuringDiskCreate(t *testing.T) {
	for _, change := range []string{"selected node", "size", "class uid"} {
		t.Run(change, func(t *testing.T) {
			f := newProvisionerFixture(t)
			f.disks.onCreate = func() {
				if change == "class uid" {
					updated := f.class.DeepCopy()
					updated.UID = "replacement-class"
					if _, err := f.client.StorageV1().StorageClasses().Update(t.Context(), updated, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
					return
				}
				claim := f.claim.DeepCopy()
				if change == "selected node" {
					claim.Annotations[selectedVolumeNode] = "other"
				} else {
					claim.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("1Gi")
				}
				f.putClaim(t, claim)
			}
			if err := f.p.provision(t.Context(), f.claim, f.class); err == nil {
				t.Fatal("changed admission published a PV")
			}
			volumes, err := f.client.CoreV1().PersistentVolumes().List(t.Context(), metav1.ListOptions{})
			if err != nil || len(volumes.Items) != 0 {
				t.Fatal("PV published after admission changed")
			}
			if f.disks.removes != 0 || len(f.disks.disks) != 1 {
				t.Fatal("changed admission deleted unknown backing storage")
			}
		})
	}
}
func TestPersistentProvisionerRetainDeletionPreservesDiskAndMissingPVIsNotRecreated(t *testing.T) {
	f := newProvisionerFixture(t)
	pv := f.provision(t)
	f.bind(t, pv)
	pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	pv.DeletionTimestamp = new(metav1.Now())
	f.putPV(t, pv)
	if err := f.p.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err := f.client.CoreV1().PersistentVolumes().Get(t.Context(), pv.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Finalizers) != 0 || f.disks.removes != 0 {
		t.Fatal("Retain deleted storage or kept its finalizer")
	}
	f = newProvisionerFixture(t)
	pv = f.provision(t)
	f.bind(t, pv)
	if err := f.client.Tracker().Delete(provisionerPVResource, "", pv.Name); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	if err := f.p.reconcile(t.Context()); err == nil {
		t.Fatal("missing PV was accepted")
	}
	if f.disks.creates != 1 || f.disks.removes != 0 {
		t.Fatal("missing PV replaced or reclaimed disk")
	}
}
func TestPersistentProvisionerRefusesWrongControllerIdentity(t *testing.T) {
	f := newProvisionerFixture(t)
	f.provision(t)
	if _, err := newPersistentProvisioner(f.client, f.disks, f.state, "other", "cluster"); err == nil {
		t.Fatal("another node adopted storage")
	}
	if _, err := newPersistentProvisioner(f.client, f.disks, f.state, "native", "other"); err == nil {
		t.Fatal("another cluster adopted storage")
	}
}
