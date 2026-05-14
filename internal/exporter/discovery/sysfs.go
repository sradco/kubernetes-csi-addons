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
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// maxSysfsFileSize limits reads from sysfs pseudo-files (e.g. dm/uuid).
const maxSysfsFileSize = 256

// ResolveDeviceName resolves a major:minor pair to a kernel block device name
// by reading the symlink at /sys/dev/block/{major}:{minor}.
func ResolveDeviceName(sysPath string, major, minor uint32) (string, error) {
	link := filepath.Join(sysPath, "dev", "block", fmt.Sprintf("%d:%d", major, minor))
	target, err := os.Readlink(link)
	if err != nil {
		return "", fmt.Errorf("readlink %s: %w", link, err)
	}
	return filepath.Base(target), nil
}

// ResolveMultipathDevice checks whether a dm device is a multipath device or
// has an immediate multipath slave (the LUKS-over-multipath case). This covers
// the two device stacks that supported CSI drivers produce:
//
//	Direct:  CSI mount → dm-0 (mpath)
//	LUKS:    CSI mount → dm-1 (LUKS) → dm-0 (mpath)
//
// If the device is already multipath, it is returned as-is.
// If a multipath slave is found one level down, that slave is returned.
// Otherwise the original device is returned unchanged — the exporter emits
// whatever device it finds and lets the PromQL join determine whether
// upstream path-health metrics exist for it.
func ResolveMultipathDevice(sysPath string, device string) string {
	if !strings.HasPrefix(device, "dm-") {
		return device
	}

	if isMpath, _ := isMultipathDevice(sysPath, device); isMpath {
		return device
	}

	slavesDir := filepath.Join(sysPath, "block", device, "slaves")
	entries, err := os.ReadDir(slavesDir)
	if err != nil {
		return device
	}

	for _, entry := range entries {
		slave := entry.Name()
		if !strings.HasPrefix(slave, "dm-") {
			continue
		}
		if isMpath, _ := isMultipathDevice(sysPath, slave); isMpath {
			return slave
		}
	}

	return device
}

// ResolveLUKSUnderlyingDevice is an alias for ResolveMultipathDevice,
// kept for backward compatibility with the HPE and Trident discoverers.
func ResolveLUKSUnderlyingDevice(sysPath string, device string) string {
	return ResolveMultipathDevice(sysPath, device)
}

func isMultipathDevice(sysPath string, device string) (bool, error) {
	uuidPath := filepath.Join(sysPath, "block", device, "dm", "uuid")
	data, err := readFileLimited(uuidPath, maxSysfsFileSize)
	if err != nil {
		return false, err
	}
	return strings.HasPrefix(strings.TrimSpace(string(data)), "mpath-"), nil
}
