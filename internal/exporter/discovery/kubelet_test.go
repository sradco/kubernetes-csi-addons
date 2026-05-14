/*
Copyright 2025 The Kubernetes-CSI-Addons Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// fakeStatMount returns a statMountDevFunc that maps known paths to fake
// major:minor values, simulating mount boundaries detected by stat().
func fakeStatMount(mounts map[string][2]uint32) statMountDevFunc {
	return func(path string) (uint32, uint32, error) {
		if dev, ok := mounts[path]; ok {
			return dev[0], dev[1], nil
		}
		return 0, 0, fmt.Errorf("no mount detected at %s", path)
	}
}

func TestKubeletDiscoverer_FilesystemVolume(t *testing.T) {
	tmpDir := t.TempDir()
	kubeletRoot := filepath.Join(tmpDir, "kubelet")
	sysDir := filepath.Join(tmpDir, "sys")

	driverName := "csi-powerstore.dellemc.com"
	pvName := "pvc-vol-001"
	pvDir := filepath.Join(kubeletRoot, "plugins", "kubernetes.io", "csi", driverName, pvName)
	globalMount := filepath.Join(pvDir, "globalmount")
	if err := os.MkdirAll(globalMount, 0755); err != nil {
		t.Fatal(err)
	}

	vd := volData{
		VolumeHandle:        "csi-vol-handle-001",
		DriverName:          driverName,
		SpecVolID:           pvName,
		VolumeLifecycleMode: "Persistent",
	}
	data, _ := json.Marshal(vd)
	if err := os.WriteFile(filepath.Join(pvDir, "vol_data.json"), data, 0644); err != nil {
		t.Fatal(err)
	}

	devBlockDir := filepath.Join(sysDir, "dev", "block")
	if err := os.MkdirAll(devBlockDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../devices/virtual/block/dm-5", filepath.Join(devBlockDir, "253:5")); err != nil {
		t.Fatal(err)
	}
	dmUUIDDir := filepath.Join(sysDir, "block", "dm-5", "dm")
	if err := os.MkdirAll(dmUUIDDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dmUUIDDir, "uuid"), []byte("mpath-abc\n"), 0644); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	d := NewKubeletDiscoverer(kubeletRoot, sysDir, "worker-1", logger)
	d.statMount = fakeStatMount(map[string][2]uint32{
		globalMount: {253, 5},
	})

	results, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	r := results[0]
	if r.VolumeHandle != "csi-vol-handle-001" {
		t.Errorf("expected volume_handle csi-vol-handle-001, got %s", r.VolumeHandle)
	}
	if r.Driver != "csi-powerstore.dellemc.com" {
		t.Errorf("expected driver csi-powerstore.dellemc.com, got %s", r.Driver)
	}
	if r.Device != "dm-5" {
		t.Errorf("expected device dm-5, got %s", r.Device)
	}
	if r.Node != "worker-1" {
		t.Errorf("expected node worker-1, got %s", r.Node)
	}
}

func TestKubeletDiscoverer_SkipsEphemeral(t *testing.T) {
	tmpDir := t.TempDir()
	kubeletRoot := filepath.Join(tmpDir, "kubelet")
	sysDir := filepath.Join(tmpDir, "sys")

	pvDir := filepath.Join(kubeletRoot, "plugins", "kubernetes.io", "csi", "csi.example.com", "ephemeral-vol")
	if err := os.MkdirAll(filepath.Join(pvDir, "globalmount"), 0755); err != nil {
		t.Fatal(err)
	}

	vd := volData{
		VolumeHandle:        "pod-uid-123",
		DriverName:          "csi.example.com",
		SpecVolID:           "ephemeral-vol",
		VolumeLifecycleMode: "Ephemeral",
	}
	data, _ := json.Marshal(vd)
	if err := os.WriteFile(filepath.Join(pvDir, "vol_data.json"), data, 0644); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	d := NewKubeletDiscoverer(kubeletRoot, sysDir, "worker-1", logger)
	d.statMount = fakeStatMount(nil)

	results, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for ephemeral volume, got %d", len(results))
	}
}

func TestKubeletDiscoverer_SkipsNoMount(t *testing.T) {
	tmpDir := t.TempDir()
	kubeletRoot := filepath.Join(tmpDir, "kubelet")
	sysDir := filepath.Join(tmpDir, "sys")

	pvDir := filepath.Join(kubeletRoot, "plugins", "kubernetes.io", "csi", "csi.example.com", "vol-no-mount")
	if err := os.MkdirAll(filepath.Join(pvDir, "globalmount"), 0755); err != nil {
		t.Fatal(err)
	}

	vd := volData{
		VolumeHandle:        "no-mount-handle",
		DriverName:          "csi.example.com",
		SpecVolID:           "vol-no-mount",
		VolumeLifecycleMode: "Persistent",
	}
	data, _ := json.Marshal(vd)
	if err := os.WriteFile(filepath.Join(pvDir, "vol_data.json"), data, 0644); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	d := NewKubeletDiscoverer(kubeletRoot, sysDir, "worker-1", logger)
	// No mounts registered — statMount will return error for all paths.
	d.statMount = fakeStatMount(nil)

	results, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results when no mount boundary detected, got %d", len(results))
	}
}

func TestKubeletDiscoverer_PublishMount(t *testing.T) {
	tmpDir := t.TempDir()
	kubeletRoot := filepath.Join(tmpDir, "kubelet")
	sysDir := filepath.Join(tmpDir, "sys")

	driverName := "csi.example.com"
	specVolID := "pvc-publish-001"

	pvDir := filepath.Join(kubeletRoot, "plugins", "kubernetes.io", "csi", driverName, specVolID)
	if err := os.MkdirAll(pvDir, 0755); err != nil {
		t.Fatal(err)
	}
	vd := volData{
		VolumeHandle: "publish-vol-handle",
		DriverName:   driverName,
		SpecVolID:    specVolID,
	}
	data, _ := json.Marshal(vd)
	if err := os.WriteFile(filepath.Join(pvDir, "vol_data.json"), data, 0644); err != nil {
		t.Fatal(err)
	}

	podMountDir := filepath.Join(kubeletRoot, "pods", "pod-uid-456", "volumes", "kubernetes.io~csi", specVolID, "mount")
	if err := os.MkdirAll(podMountDir, 0755); err != nil {
		t.Fatal(err)
	}

	devBlockDir := filepath.Join(sysDir, "dev", "block")
	if err := os.MkdirAll(devBlockDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../block/sdb", filepath.Join(devBlockDir, "8:16")); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	d := NewKubeletDiscoverer(kubeletRoot, sysDir, "worker-1", logger)
	d.statMount = fakeStatMount(map[string][2]uint32{
		podMountDir: {8, 16},
	})

	results, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result from publish mount, got %d", len(results))
	}
	if results[0].VolumeHandle != "publish-vol-handle" {
		t.Errorf("expected volume_handle publish-vol-handle, got %s", results[0].VolumeHandle)
	}
	if results[0].Device != "sdb" {
		t.Errorf("expected device sdb, got %s", results[0].Device)
	}
}

func TestKubeletDiscoverer_LUKSOverMultipath(t *testing.T) {
	tmpDir := t.TempDir()
	kubeletRoot := filepath.Join(tmpDir, "kubelet")
	sysDir := filepath.Join(tmpDir, "sys")

	driverName := "csi-powerstore.dellemc.com"
	pvName := "pvc-luks-001"
	pvDir := filepath.Join(kubeletRoot, "plugins", "kubernetes.io", "csi", driverName, pvName)
	globalMount := filepath.Join(pvDir, "globalmount")
	if err := os.MkdirAll(globalMount, 0755); err != nil {
		t.Fatal(err)
	}

	vd := volData{
		VolumeHandle:        "luks-vol-handle",
		DriverName:          driverName,
		SpecVolID:           pvName,
		VolumeLifecycleMode: "Persistent",
	}
	data, _ := json.Marshal(vd)
	if err := os.WriteFile(filepath.Join(pvDir, "vol_data.json"), data, 0644); err != nil {
		t.Fatal(err)
	}

	// stat() returns dm-1 (LUKS device) at 253:1
	devBlockDir := filepath.Join(sysDir, "dev", "block")
	if err := os.MkdirAll(devBlockDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../devices/virtual/block/dm-1", filepath.Join(devBlockDir, "253:1")); err != nil {
		t.Fatal(err)
	}

	// dm-1 is LUKS, with slave dm-0
	luksDir := filepath.Join(sysDir, "block", "dm-1", "dm")
	if err := os.MkdirAll(luksDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(luksDir, "uuid"), []byte("CRYPT-LUKS2-xxx\n"), 0644); err != nil {
		t.Fatal(err)
	}
	luksSlavesDir := filepath.Join(sysDir, "block", "dm-1", "slaves")
	if err := os.MkdirAll(filepath.Join(luksSlavesDir, "dm-0"), 0755); err != nil {
		t.Fatal(err)
	}

	// dm-0 is multipath
	mpathDir := filepath.Join(sysDir, "block", "dm-0", "dm")
	if err := os.MkdirAll(mpathDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mpathDir, "uuid"), []byte("mpath-abcdef\n"), 0644); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	d := NewKubeletDiscoverer(kubeletRoot, sysDir, "worker-1", logger)
	d.statMount = fakeStatMount(map[string][2]uint32{
		globalMount: {253, 1},
	})

	results, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Device != "dm-0" {
		t.Errorf("expected device dm-0 (multipath under LUKS), got %s", results[0].Device)
	}
}
