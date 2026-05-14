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
	"os"
	"path/filepath"
	"testing"
)

func TestResolveDeviceName(t *testing.T) {
	sysPath := t.TempDir()

	blockDir := filepath.Join(sysPath, "dev", "block")
	if err := os.MkdirAll(blockDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../block/dm-5", filepath.Join(blockDir, "253:5")); err != nil {
		t.Fatal(err)
	}

	device, err := ResolveDeviceName(sysPath, 253, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if device != "dm-5" {
		t.Errorf("expected dm-5, got %s", device)
	}
}

func TestResolveDeviceName_NotFound(t *testing.T) {
	sysPath := t.TempDir()
	_, err := ResolveDeviceName(sysPath, 999, 999)
	if err == nil {
		t.Fatal("expected error for non-existent device")
	}
}

func TestResolveMultipathDevice_NotDM(t *testing.T) {
	device := ResolveMultipathDevice("/fake", "sda")
	if device != "sda" {
		t.Errorf("expected sda (non-dm returned as-is), got %s", device)
	}
}

func TestResolveMultipathDevice_NVMe(t *testing.T) {
	device := ResolveMultipathDevice("/fake", "nvme0n1")
	if device != "nvme0n1" {
		t.Errorf("expected nvme0n1 (non-dm returned as-is), got %s", device)
	}
}

func TestResolveMultipathDevice_DirectMultipath(t *testing.T) {
	sysPath := t.TempDir()

	dmDir := filepath.Join(sysPath, "block", "dm-0", "dm")
	if err := os.MkdirAll(dmDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dmDir, "uuid"), []byte("mpath-12345\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	device := ResolveMultipathDevice(sysPath, "dm-0")
	if device != "dm-0" {
		t.Errorf("expected dm-0 (multipath returned as-is), got %s", device)
	}
}

func TestResolveMultipathDevice_LUKSOverMultipath(t *testing.T) {
	sysPath := t.TempDir()

	// dm-1 is LUKS
	luksDir := filepath.Join(sysPath, "block", "dm-1", "dm")
	if err := os.MkdirAll(luksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(luksDir, "uuid"), []byte("CRYPT-LUKS2-xxx\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	luksSlavesDir := filepath.Join(sysPath, "block", "dm-1", "slaves")
	if err := os.MkdirAll(filepath.Join(luksSlavesDir, "dm-0"), 0o755); err != nil {
		t.Fatal(err)
	}

	// dm-0 is multipath (one level down)
	mpathDir := filepath.Join(sysPath, "block", "dm-0", "dm")
	if err := os.MkdirAll(mpathDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mpathDir, "uuid"), []byte("mpath-abcdef\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	device := ResolveMultipathDevice(sysPath, "dm-1")
	if device != "dm-0" {
		t.Errorf("expected dm-0 (multipath one hop below LUKS), got %s", device)
	}
}

func TestResolveMultipathDevice_NonMultipathDM_ReturnedAsIs(t *testing.T) {
	sysPath := t.TempDir()

	// dm-2 is LVM, slave is sda1 (not multipath)
	dmDir := filepath.Join(sysPath, "block", "dm-2", "dm")
	if err := os.MkdirAll(dmDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dmDir, "uuid"), []byte("LVM-xxx\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	slavesDir := filepath.Join(sysPath, "block", "dm-2", "slaves")
	if err := os.MkdirAll(filepath.Join(slavesDir, "sda1"), 0o755); err != nil {
		t.Fatal(err)
	}

	device := ResolveMultipathDevice(sysPath, "dm-2")
	if device != "dm-2" {
		t.Errorf("expected dm-2 (no multipath found, returned as-is), got %s", device)
	}
}

func TestResolveMultipathDevice_NoSlaves(t *testing.T) {
	sysPath := t.TempDir()

	dmDir := filepath.Join(sysPath, "block", "dm-4", "dm")
	if err := os.MkdirAll(dmDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dmDir, "uuid"), []byte("CRYPT-LUKS2-yyy\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	device := ResolveMultipathDevice(sysPath, "dm-4")
	if device != "dm-4" {
		t.Errorf("expected dm-4 (no slaves dir, returned as-is), got %s", device)
	}
}

func TestResolveMultipathDevice_DeepStack_StopsAtOneHop(t *testing.T) {
	sysPath := t.TempDir()

	// dm-3: non-mpath, slave is dm-2
	for _, dev := range []string{"dm-3", "dm-2"} {
		dmDir := filepath.Join(sysPath, "block", dev, "dm")
		if err := os.MkdirAll(dmDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dmDir, "uuid"), []byte("LVM-xxx\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	slavesDir := filepath.Join(sysPath, "block", "dm-3", "slaves")
	if err := os.MkdirAll(filepath.Join(slavesDir, "dm-2"), 0o755); err != nil {
		t.Fatal(err)
	}

	// dm-2 has slave dm-0 which IS multipath — but that's 2 hops, not 1
	slavesDir2 := filepath.Join(sysPath, "block", "dm-2", "slaves")
	if err := os.MkdirAll(filepath.Join(slavesDir2, "dm-0"), 0o755); err != nil {
		t.Fatal(err)
	}
	mpathDir := filepath.Join(sysPath, "block", "dm-0", "dm")
	if err := os.MkdirAll(mpathDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mpathDir, "uuid"), []byte("mpath-deep\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	device := ResolveMultipathDevice(sysPath, "dm-3")
	if device != "dm-3" {
		t.Errorf("expected dm-3 (multipath 2 hops away is out of scope), got %s", device)
	}
}

// Backward compatibility alias
func TestResolveLUKSUnderlyingDevice_BackwardCompat(t *testing.T) {
	sysPath := t.TempDir()

	dmDir := filepath.Join(sysPath, "block", "dm-0", "dm")
	if err := os.MkdirAll(dmDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dmDir, "uuid"), []byte("mpath-compat\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	device := ResolveLUKSUnderlyingDevice(sysPath, "dm-0")
	if device != "dm-0" {
		t.Errorf("expected dm-0 via backward-compat alias, got %s", device)
	}
}
