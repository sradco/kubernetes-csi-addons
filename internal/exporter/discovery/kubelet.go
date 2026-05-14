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
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
)

// statMountDevFunc is the signature for the function that detects mount
// boundaries and returns the backing device's major:minor.
type statMountDevFunc func(path string) (major, minor uint32, err error)

// KubeletDiscoverer implements universal discovery using vol_data.json + stat().
// Filesystem volumes are detected by stat()ing the globalmount or publish-mount
// path and comparing st_dev with the parent directory to detect mount boundaries.
// This requires mountPropagation: HostToContainer on the kubelet hostPath mount
// so that CSI staging mounts are visible inside the container.
type KubeletDiscoverer struct {
	// containerKubeletRoot is the path where host kubelet root is mounted inside the container
	// (e.g., /host/kubelet). Used for reading files and stat()ing mount paths.
	containerKubeletRoot string
	sysPath              string
	nodeName             string
	logger               *slog.Logger
	publishPattern       *regexp.Regexp
	// statMountDev detects a mount boundary at the given path and returns
	// the backing device's major:minor. Defaults to statMountDev; overridden in tests.
	statMount statMountDevFunc
}

type volDataIndex struct {
	byDir       map[string]*volData
	bySpecVolID map[string]*volData
}

// NewKubeletDiscoverer creates a universal kubelet discoverer.
// containerKubeletRoot is where kubelet data is accessible inside the container
// (must be mounted with mountPropagation: HostToContainer so CSI staging mounts
// are visible).
func NewKubeletDiscoverer(containerKubeletRoot, sysPath, nodeName string, logger *slog.Logger) *KubeletDiscoverer {
	pattern := regexp.MustCompile(
		`/pods/[^/]+/volumes/kubernetes\.io~csi/([^/]+)/mount$`,
	)
	return &KubeletDiscoverer{
		containerKubeletRoot: containerKubeletRoot,
		sysPath:              sysPath,
		nodeName:             nodeName,
		logger:               logger,
		publishPattern:       pattern,
		statMount:            statMountDev,
	}
}

// Name implements Discoverer.
func (d *KubeletDiscoverer) Name() string { return "kubelet" }

// Discover implements Discoverer.
func (d *KubeletDiscoverer) Discover(ctx context.Context) ([]VolumeDevice, error) {
	idx, parseErrors := d.buildVolDataIndex()
	if parseErrors > 0 {
		d.logger.Warn("some vol_data.json files could not be parsed; those volumes will be missing from metrics",
			"count", parseErrors)
	}
	if idx == nil {
		idx = &volDataIndex{
			byDir:       make(map[string]*volData),
			bySpecVolID: make(map[string]*volData),
		}
	}

	// seen deduplicates across all sub-discoverers: globalmount, block-device,
	// and pod publish-mount can all match the same VolumeHandle.
	seen := make(map[string]struct{})
	var results []VolumeDevice

	appendUnique := func(vds []VolumeDevice) {
		for _, v := range vds {
			if _, ok := seen[v.VolumeHandle]; ok {
				continue
			}
			seen[v.VolumeHandle] = struct{}{}
			results = append(results, v)
		}
	}

	appendUnique(d.discoverFilesystemVolumes(ctx, idx))

	if ctx.Err() != nil {
		return results, ctx.Err()
	}

	appendUnique(d.discoverBlockVolumes(ctx))

	if ctx.Err() != nil {
		return results, ctx.Err()
	}

	appendUnique(d.discoverPublishMountVolumes(ctx, idx))

	if ctx.Err() != nil && len(results) == 0 {
		return nil, ctx.Err()
	}
	return results, nil
}

type volData struct {
	VolumeHandle        string `json:"volumeHandle"`
	DriverName          string `json:"driverName"`
	SpecVolID           string `json:"specVolID"`
	VolumeLifecycleMode string `json:"volumeLifecycleMode"`
}

// buildVolDataIndex reads all vol_data.json files once and indexes by directory name and specVolID.
// Uses containerKubeletRoot for file access.
func (d *KubeletDiscoverer) buildVolDataIndex() (*volDataIndex, int) {
	csiPluginDir := filepath.Join(d.containerKubeletRoot, "plugins", "kubernetes.io", "csi")
	driverEntries, err := os.ReadDir(csiPluginDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0
		}
		d.logger.Warn("failed to read kubelet CSI plugin dir", "path", csiPluginDir, "error", err)
		return nil, 0
	}

	idx := &volDataIndex{
		byDir:       make(map[string]*volData),
		bySpecVolID: make(map[string]*volData),
	}
	parseErrors := 0

	for _, driverEntry := range driverEntries {
		if !driverEntry.IsDir() {
			continue
		}
		driverDir := filepath.Join(csiPluginDir, driverEntry.Name())
		volumeEntries, err := os.ReadDir(driverDir)
		if err != nil {
			continue
		}
		for _, volEntry := range volumeEntries {
			if !volEntry.IsDir() {
				continue
			}
			pvDir := filepath.Join(driverDir, volEntry.Name())
			vd, err := readVolData(pvDir)
			if err != nil {
				parseErrors++
				d.logger.Warn("failed to parse vol_data.json", "dir", pvDir, "error", err)
				continue
			}
			key := filepath.Join(driverEntry.Name(), volEntry.Name())
			idx.byDir[key] = vd
			if vd.SpecVolID != "" {
				idx.bySpecVolID[vd.SpecVolID] = vd
			}
		}
	}
	return idx, parseErrors
}

// discoverFilesystemVolumes detects CSI filesystem volumes by stat()ing
// the globalmount path. A mount boundary is detected when st_dev of the
// globalmount (or a direct child) differs from its parent directory.
// This requires mountPropagation: HostToContainer on the kubelet hostPath mount.
func (d *KubeletDiscoverer) discoverFilesystemVolumes(ctx context.Context, idx *volDataIndex) []VolumeDevice {
	containerCSIDir := filepath.Join(d.containerKubeletRoot, "plugins", "kubernetes.io", "csi")

	var results []VolumeDevice
	for dirName, vd := range idx.byDir {
		if ctx.Err() != nil {
			return results
		}
		if vd.VolumeLifecycleMode == "Ephemeral" {
			continue
		}

		globalMount := filepath.Join(containerCSIDir, dirName, "globalmount")
		major, minor, err := d.statMount(globalMount)
		if err != nil {
			continue
		}
		if isNetworkFSByMagic(globalMount) {
			continue
		}

		device, err := d.resolveDevice(int(major), int(minor))
		if err != nil {
			d.logger.Warn("device resolution failed",
				"volume_handle", vd.VolumeHandle,
				"major", major,
				"minor", minor,
				"error", err)
			continue
		}

		results = append(results, VolumeDevice{
			VolumeHandle: vd.VolumeHandle,
			Driver:       vd.DriverName,
			Device:       device,
			Node:         d.nodeName,
		})
	}
	return results
}

// discoverBlockVolumes uses containerKubeletRoot for file access.
func (d *KubeletDiscoverer) discoverBlockVolumes(ctx context.Context) []VolumeDevice {
	blockDir := filepath.Join(d.containerKubeletRoot, "plugins", "kubernetes.io", "csi", "volumeDevices")
	entries, err := os.ReadDir(blockDir)
	if err != nil {
		return nil
	}

	var results []VolumeDevice
	for _, entry := range entries {
		if ctx.Err() != nil {
			return results
		}
		if !entry.IsDir() {
			continue
		}
		pvDir := filepath.Join(blockDir, entry.Name())
		vd, err := readVolData(pvDir)
		if err != nil {
			continue
		}
		if vd.VolumeLifecycleMode == "Ephemeral" {
			continue
		}

		devFile := filepath.Join(pvDir, "dev", vd.SpecVolID)
		var stat unix.Stat_t
		if err := unix.Stat(devFile, &stat); err != nil {
			continue
		}
		major := unix.Major(stat.Rdev)
		minor := unix.Minor(stat.Rdev)

		device, err := d.resolveDevice(int(major), int(minor))
		if err != nil {
			continue
		}

		results = append(results, VolumeDevice{
			VolumeHandle: vd.VolumeHandle,
			Driver:       vd.DriverName,
			Device:       device,
			Node:         d.nodeName,
		})
	}
	return results
}

// discoverPublishMountVolumes walks pod CSI volume directories and detects
// publish mounts via stat(). A mount is detected when st_dev of the mount
// directory differs from its parent.
func (d *KubeletDiscoverer) discoverPublishMountVolumes(ctx context.Context, idx *volDataIndex) []VolumeDevice {
	podsDir := filepath.Join(d.containerKubeletRoot, "pods")
	podEntries, err := os.ReadDir(podsDir)
	if err != nil {
		return nil
	}

	var results []VolumeDevice
	for _, podEntry := range podEntries {
		if ctx.Err() != nil {
			return results
		}
		if !podEntry.IsDir() {
			continue
		}
		csiVolDir := filepath.Join(podsDir, podEntry.Name(), "volumes", "kubernetes.io~csi")
		volEntries, err := os.ReadDir(csiVolDir)
		if err != nil {
			continue
		}
		for _, volEntry := range volEntries {
			if !volEntry.IsDir() {
				continue
			}
			mountPath := filepath.Join(csiVolDir, volEntry.Name(), "mount")
			matches := d.publishPattern.FindStringSubmatch(mountPath)
			if matches == nil {
				continue
			}

			major, minor, err := d.statMount(mountPath)
			if err != nil {
				continue
			}
			if isNetworkFSByMagic(mountPath) {
				continue
			}

			specVolID := matches[1]
			vd := idx.bySpecVolID[specVolID]
			if vd == nil {
				vd = idx.byDir[specVolID]
			}
			if vd == nil {
				continue
			}

			device, err := d.resolveDevice(int(major), int(minor))
			if err != nil {
				continue
			}

			results = append(results, VolumeDevice{
				VolumeHandle: vd.VolumeHandle,
				Driver:       vd.DriverName,
				Device:       device,
				Node:         d.nodeName,
			})
		}
	}
	return results
}

// statMountDev stat()s the given path and its parent to detect a mount
// boundary. If the path has a different st_dev than its parent, it is a
// mount point and the function returns the major:minor of the backing device.
// For paths where a CSI driver stages the volume under a subdirectory of
// globalmount (e.g. .../globalmount/<volumeHandle>), the function also checks
// direct children when the path itself is not a mount boundary.
func statMountDev(path string) (major, minor uint32, err error) {
	var pathStat unix.Stat_t
	if err := unix.Stat(path, &pathStat); err != nil {
		return 0, 0, err
	}

	var parentStat unix.Stat_t
	parentDir := filepath.Dir(path)
	if err := unix.Stat(parentDir, &parentStat); err != nil {
		return 0, 0, err
	}

	if pathStat.Dev != parentStat.Dev {
		return unix.Major(pathStat.Dev), unix.Minor(pathStat.Dev), nil
	}

	// Check direct children for CSI drivers that stage under a subdirectory.
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, 0, fmt.Errorf("no mount detected at %s", path)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		childPath := filepath.Join(path, entry.Name())
		var childStat unix.Stat_t
		if err := unix.Stat(childPath, &childStat); err != nil {
			continue
		}
		if childStat.Dev != pathStat.Dev {
			return unix.Major(childStat.Dev), unix.Minor(childStat.Dev), nil
		}
	}

	return 0, 0, fmt.Errorf("no mount detected at %s", path)
}

// isNetworkFSByMagic uses statfs() to detect network filesystems by their
// magic number, replacing the mountinfo FSType check.
func isNetworkFSByMagic(path string) bool {
	var buf unix.Statfs_t
	if err := unix.Statfs(path, &buf); err != nil {
		return false
	}
	switch buf.Type {
	case
		0x6969,     // NFS_SUPER_MAGIC
		0xFF534D42, // CIFS_SUPER_MAGIC
		0x65735546, // FUSE_SUPER_MAGIC (GlusterFS, CephFS-FUSE, etc.)
		0x00C36400, // CEPH_SUPER_MAGIC
		0x0BD00BD0, // LUSTRE_SUPER_MAGIC
		0x01021997: // V9FS_MAGIC (9P)
		return true
	}
	return false
}

func readVolData(pvDir string) (*volData, error) {
	data, err := readFileLimited(filepath.Join(pvDir, "vol_data.json"), maxJSONFileSize)
	if err != nil {
		return nil, err
	}
	var vd volData
	if err := json.Unmarshal(data, &vd); err != nil {
		return nil, err
	}
	if vd.VolumeHandle == "" || vd.DriverName == "" {
		return nil, ErrMissingFields
	}
	return &vd, nil
}

func (d *KubeletDiscoverer) resolveDevice(major, minor int) (string, error) {
	device, err := ResolveDeviceName(d.sysPath, uint32(major), uint32(minor))
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(device, "dm-") {
		return ResolveMultipathDevice(d.sysPath, device), nil
	}
	return device, nil
}
