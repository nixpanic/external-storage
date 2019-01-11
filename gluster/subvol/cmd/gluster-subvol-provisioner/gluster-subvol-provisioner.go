/*
Copyright 2018 The Kubernetes Authors.

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

package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/containerd/continuity/fs"
	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/lib/controller"
	"github.com/kubernetes-incubator/external-storage/lib/gidallocator"
	"github.com/kubernetes-incubator/external-storage/lib/util"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

const (
	provisionerName    = "gluster.org/gluster-subvol"
	provisionerNameKey = "PROVISIONER_NAME"
	descAnn            = "Gluster-subvol: Dynamically provisioned PV"
	dynamicEpSvcPrefix = "gluster-subvol-dynamic-"
	glusterTypeAnn     = "gluster.org/type"
	parentPVCAnn       = "gluster.org/parentpvc"
	gidAnn             = "pv.beta.kubernetes.io/gid"
	workdir            = "/var/run/gluster-subvol"
	unmountDelay       = "AUTO_UNMOUNT_DELAY"

	// CloneRequestAnn is an annotation to request that the PVC be provisioned as a clone of the referenced PVC
	CloneRequestAnn = "k8s.io/CloneRequest"

	// CloneOfAnn is an annotation to indicate that a PVC is a clone of the referenced PVC
	CloneOfAnn = "k8s.io/CloneOf"

	// status of cloning ("Started", "Failed", "Finished")
	// TODO: make this a string-enum
	cloneStatusAnn = "subvol.gluster.org/CloneStatus"
)

// glusterMountEntry is stored in the glusterSubvolProvisioner.mtab map.
type glusterMountEntry struct {
	mountpoint string
        // timestamp of last access, for unmounting unused supervols
	atime time.Time
}

type glusterSubvolProvisioner struct {
	client    kubernetes.Interface
	identity  string
	allocator gidallocator.Allocator

	// changes to the mtab map need to be done under mtabLock
	mtabLock     *sync.Mutex
	mtab         map[string]glusterMountEntry
	mountTimeout int // expire mounted supervol PVs in ... seconds

	// channel to stop the autoUnmounter go-routine
	autoUnmounterC chan<- interface{}
}

var _ controller.Provisioner = &glusterSubvolProvisioner{}

var accessModes = []v1.PersistentVolumeAccessMode{
	v1.ReadWriteMany,
	v1.ReadOnlyMany,
	v1.ReadWriteOnce,
}

func newGlusterSubvolProvisioner(client kubernetes.Interface, id string, timeout int) (controller.Provisioner, *glusterSubvolProvisioner, error) {
	p := &glusterSubvolProvisioner{
		client:       client,
		identity:     id,
		allocator:    gidallocator.New(client),
		mtabLock:     &sync.Mutex{},
		mtab:         make(map[string]glusterMountEntry),
		mountTimeout: timeout,
	}

	// start the go routine to automatically unmount supervol PVs
	p.autoUnmounter()

	return p, p, nil
}

func (p *glusterSubvolProvisioner) getPVC(ns string, name string) (*v1.PersistentVolumeClaim, error) {
	return p.client.CoreV1().PersistentVolumeClaims(ns).Get(name, metav1.GetOptions{})
}

func (p *glusterSubvolProvisioner) annotatePVC(ns, pvc string, updates map[string]string) error {
	// Retrieve the latest version of PVC before attempting update
	// RetryOnConflict uses exponential backoff to avoid exhausting the apiserver
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, getErr := p.getPVC(ns, pvc)
		if getErr != nil {
			panic(fmt.Errorf("Failed to get latest version of PVC %s/%s: %s", ns, pvc, getErr))
		}

		for k, v := range updates {
			result.Annotations[k] = v
		}
		_, updateErr := p.client.CoreV1().PersistentVolumeClaims(ns).Update(result)
		return updateErr
	})
	if retryErr != nil {
		glog.Errorf("Update of PVC %s/%s failed: %s", ns, pvc, retryErr)
		return retryErr
	}
	return nil
}

// unmount a path, return an error if unmounting fails
func (p *glusterSubvolProvisioner) umount(mountpoint string) error {
	umountCmd := exec.Command("/bin/umount", mountpoint)
	glog.Infof("going to unmount: %s", mountpoint)
	err := umountCmd.Run()
	if err != nil {
		glog.Errorf("failed to unmount: %s", err)
		return err
	}

	return nil
}

// unmount all mounted supervols
func (p *glusterSubvolProvisioner) umountAll() {
	// this whole function modifies mtab, lock it
	p.mtabLock.Lock()
	defer p.mtabLock.Unlock()

	for pv, mountentry := range p.mtab {
		err := p.umount(mountentry.mountpoint)
		if err == nil {
			delete(p.mtab, pv)
		}
	}
}

// unmount the given PV, return an error if unmounting fails
func (p *glusterSubvolProvisioner) umountPV(pv string) error {
	// this whole function checks and potentially modifies mtab, lock it
	p.mtabLock.Lock()
	defer p.mtabLock.Unlock()

	// check if the pv is mounted at all
	mountentry, mounted := p.mtab[pv]
	if !mounted {
		return fmt.Errorf("can not unmount PV %s: not mounted", pv)
	}

	err := p.umount(mountentry.mountpoint)
	if err != nil {
		glog.Errorf("failed to unmount PV %s: %s", pv, err)
		return err
	}

	return nil
}

func (p *glusterSubvolProvisioner) expireMounts() {
	// this whole function checks and potentially modifies mtab, lock it
	p.mtabLock.Lock()
	defer p.mtabLock.Unlock()

	// Time.Add() accepts negative values for subtracting
	expireTime := time.Now().Add(time.Second * time.Duration(p.mountTimeout) * -1)

	for pv, mountentry := range p.mtab {
		// if the atime is before the expireTime, unmount it
		if mountentry.atime.Before(expireTime) {
			err := p.umount(mountentry.mountpoint)
			if err == nil {
				delete(p.mtab, pv)
			}
		}
	}
}

// automatically unmount supervol PVs when they are not used anymore
func (p *glusterSubvolProvisioner) autoUnmounter() {
	// check for expired supervol PV mounts every 30 seconds
        ticker := time.NewTicker(time.Second * 30)
        c := make(chan interface{})
        p.autoUnmounterC = c

        go func() {
                glog.Infof("starting autoUnmunter...")
                defer ticker.Stop()
                for {
                        select {
                        case <-c:
                                glog.Infof("stopping autoUnmounter...")
                                return
                        case <-ticker.C:
                                glog.V(2).Infof("autoUnmounter: checking for expired mountpoints")
				p.expireMounts()
                        }
                }
        }()
}

// mount the given PV if not mounted yet, return the mountpoint or an error
func (p *glusterSubvolProvisioner) mountPV(ns string, pv *v1.PersistentVolume) (string, error) {
	// this whole function checks and potentially modifies mtab, lock it
	p.mtabLock.Lock()
	defer p.mtabLock.Unlock()

	// check if not mounted yet
	mountentry, mounted := p.mtab[pv.Name]
	if mounted {
		// mounted already, update the access time
		mountentry.atime = time.Now()
		return mountentry.mountpoint, nil
	}

	// create the missing mountpoint
	mountpoint := workdir + "/" + pv.Name
	sb, err := os.Stat(mountpoint)
	if err != nil {
		// mountpoint does not exist yet, create it
		glog.Infof("mountpoint %s for PV %s does not exist yet? Error: %s", mountpoint, pv.Name, err)

		err = os.MkdirAll(mountpoint, 0777)
		if err != nil {
			return "", fmt.Errorf("failed to create mountpoint %s for PV %s: %s", mountpoint, pv.Name, err)
		}
	} else if !sb.Mode().IsDir() {
		// mountpoint should be a directory, but it is not?!
		return "", fmt.Errorf("mountpoint %s for PV %s is not a directory?", mountpoint, pv.Name)
	}

	// get the name of the supervol to mount
	supervol := pv.Spec.Glusterfs.Path

	// get the mount options for the supervol
	var mountOpts string
	if len(pv.Spec.MountOptions) != 0 {
		mountOpts = strings.Join(pv.Spec.MountOptions, ",")
	}

	// Gluster Storage servers
	ep, err := p.client.CoreV1().Endpoints(ns).Get(pv.Spec.Glusterfs.EndpointsName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("endpoint %s for PV %s count not be found: %s", pv.Spec.Glusterfs.EndpointsName, pv.Name, err)
	}

	// add the additional servers as backup-volfile-servers to the mountOpts
	if len(ep.Subsets[0].Addresses) > 1 {
		var backupVolfileServers string
		for _, addr := range ep.Subsets[0].Addresses[1:] {
			if backupVolfileServers != "" {
				backupVolfileServers = backupVolfileServers + ":"
			}
			backupVolfileServers = backupVolfileServers + addr.IP
		}

		if mountOpts != "" {
			mountOpts = mountOpts + ","
		}
		mountOpts = mountOpts + "backup-volfile-servers=" + backupVolfileServers
	}

	mountCmd := make([]string, 0, 7)
	mountCmd = append(mountCmd, "/bin/mount", "-t", "glusterfs")

	// don't forget the "-o" option before the mount options string
	if mountOpts != "" {
		mountCmd = append(mountCmd, "-o", mountOpts)
	}

	mountSource := fmt.Sprintf("%s:%s", ep.Subsets[0].Addresses[0].IP, supervol)

	mountCmd = append(mountCmd, mountSource, mountpoint)
	glog.Infof("going to mount supervol %s for PV %s: %s", supervol, pv.Name, mountCmd)

	// os.StartProcess() does not allow deamons (fuse) to continue, using
	// exec.Command instead.
	cmd := exec.Command(mountCmd[0], mountCmd[1:]...)
	err = cmd.Start()
	if err != nil {
		glog.Errorf("failed to mount supervol %s: %s", supervol, err)
		return "", err
	}

	err = cmd.Wait()
	if err != nil {
		return "", fmt.Errorf("failed to mount supervol %s, process exited with %v", supervol, err)
	}

	p.mtab[pv.Name] = glusterMountEntry{
		mountpoint: mountpoint,
		atime: time.Now(),
	}

	return mountpoint, nil
}

func (p *glusterSubvolProvisioner) makeSubvolPath(mountpoint, pvcNS string, pvcUID types.UID) string {
	return fmt.Sprintf("%s/%s/pvc-%s", mountpoint, pvcNS, pvcUID)
}

func (p *glusterSubvolProvisioner) makeMountPath(supervolPV *v1.PersistentVolume, pvcNS string, pvcUID types.UID) string {
	return fmt.Sprintf("%s/%s/pvc-%s", supervolPV.Spec.Glusterfs.Path, pvcNS, pvcUID)
}

func (p *glusterSubvolProvisioner) copyEndpoints(sourceNS string, sourcePV *v1.PersistentVolume, destNS string, destPVCName string) (*v1.Endpoints, error) {
	// Need to copy the endpoints from the supervol to the new PVC. A
	// reference of the endpoints name is not sufficient, it can be in an
	// other namespace.
	sourceEP, err := p.client.CoreV1().Endpoints(sourceNS).Get(sourcePV.Spec.Glusterfs.EndpointsName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("endpoint %s for source PV %s can not be found: %s", sourcePV.Spec.Glusterfs.EndpointsName, sourcePV.Name, err)
	}

	ep := sourceEP.DeepCopy()
	ep.ObjectMeta = metav1.ObjectMeta{
		Namespace: destNS,
		Name:      dynamicEpSvcPrefix + destPVCName,
		Labels: map[string]string{
			"gluster.org/provisioned-for-pvc": destPVCName,
		},
	}

	_, err = p.client.CoreV1().Endpoints(destNS).Create(ep)
	if err != nil && errors.IsAlreadyExists(err) {
		glog.Infof("endpoint %s already exist in namespace %s, that is ok", destPVCName, destNS)
		err = nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to create endpoint %s: %s", ep.ObjectMeta.Name, err)
	}

	return ep, nil
}

// Pre-check for the requirements of the PVC-request and the supervol PVC.
// - certain features of the PVC-request may not be supported
// - the supervol PVC has some requirements that it needs to fulfil
func (p *glusterSubvolProvisioner) validateProvisionRequirements(pvcReq, supervolPVC *v1.PersistentVolumeClaim) error {
	if pvcReq.Spec.Selector != nil {
		return fmt.Errorf("claim Selector is not supported")
	}

	if !util.AccessModesContainedInAll(accessModes, pvcReq.Spec.AccessModes) {
		return fmt.Errorf("invalid AccessModes %v: only AccessModes %v are supported", pvcReq.Spec.AccessModes, accessModes)
	}

	// check permission mode, the PV should be RWX as it is mounted here,
	// and we'll create subdirs as new PVCs
	if !util.AccessModesContains(supervolPVC.Spec.AccessModes, v1.ReadWriteMany) {
		return fmt.Errorf("this provisioner requires the PVC %s/%s to have ReadWriteMany permissions", supervolPVC.Namespace, supervolPVC.Name)
	}

	return nil
}

func (p *glusterSubvolProvisioner) isCloning(pvc *v1.PersistentVolumeClaim) (bool) {
	cloneStatus, ok := pvc.Annotations[cloneStatusAnn]
	if !ok {
		// no CloneStatus annotation
		return false
	}

	switch cloneStatus {
	case "Started":
		glog.Infof("clone process for %s/%s has been started already", pvc.Namespace, pvc.Name)
	case "Failed":
		glog.Infof("clone process for %s/%s failed, restarting", pvc.Namespace, pvc.Name)
		return false
	case "Finished":
		glog.Infof("clone process for %s/%s has finished, no need to retry", pvc.Namespace, pvc.Name)
	}

	return true
}

// Check if the PVC has a CloneRequest annotation and try to clone if it has.
// This returns the source namespace/PVC that got cloned, an error if something fails.
func (p *glusterSubvolProvisioner) tryClone(options controller.VolumeOptions, supervol *v1.PersistentVolumeClaim, mountpoint, destDir string, gid *int) (bool, error) {
	pvc := options.PVC

	// sourcePVCRef points to the PVC that should get cloned
	sourcePVCRef, ok := pvc.Annotations[CloneRequestAnn]
	if !ok {
		// no CloneRequest, no need to try and clone
		glog.Infof("no %s annotation for PVC %s/%s, not going to clone", CloneRequestAnn, pvc.Namespace, pvc.Name)
		return false, nil
	}
	if sourcePVCRef == "" {
		// no CloneRequest, no need to try and clone
		glog.Infof("annotation %s for PVC %s/%s is empty, not going to clone", CloneRequestAnn, pvc.Namespace, pvc.Name)
		return false, nil
	}

	glog.Infof("got requested to clone %s for PVC %s/%s", sourcePVCRef, pvc.Namespace, pvc.Name)

	// start doClone() in the background
	go p.doClone(options, supervol, sourcePVCRef, mountpoint, destDir, gid)

	return true, nil
}

func (p *glusterSubvolProvisioner) doClone(options controller.VolumeOptions, supervolPVC *v1.PersistentVolumeClaim, sourcePVCRef, mountpoint, destDir string, gid *int) {
	pvc := options.PVC

	cloneStatus := map[string]string{
		cloneStatusAnn: "Started",
	}
	_ = p.annotatePVC(pvc.Namespace, pvc.Name, cloneStatus)

	cloneStatus[cloneStatusAnn] = "Failed"
	defer func() {
		_ = p.annotatePVC(pvc.Namespace, pvc.Name, cloneStatus)
	}()

	// sourcePVCRef is like (namespace/)?pvc
	var sourceNS, sourcePVCName string

	parts := strings.Split(sourcePVCRef, "/")
	if len(parts) == 1 {
		sourceNS = pvc.Namespace
		sourcePVCName = parts[0]
	} else if len(parts) == 2 {
		sourceNS = parts[0]
		sourcePVCName = parts[1]
	} else {
		glog.Errorf("failed to parse namespace/pvc from %s", sourcePVCRef)
	}

	// TODO: check if the size of the PVC >= source

	sourcePVC, err := p.getPVC(sourceNS, sourcePVCName)
	if err != nil {
		glog.Errorf("failed to find PVC %s/%s: %s", sourceNS, sourcePVCName, err)
	}

	// verify that the sourcePVC is on the supervolPVC
	sourceDir := p.makeSubvolPath(mountpoint, sourceNS, sourcePVC.UID)
	st, err := os.Stat(sourceDir)
	if err != nil || !st.Mode().IsDir() {
		glog.Errorf("failed to clone %s, path does not exist on supervol", sourcePVCRef)
	}

	// verification has been done!

	// optimized copy, uses copy_file_range() if possible
	err = fs.CopyDir(destDir, sourceDir, fs.WithAllowXAttrErrors())
	if err != nil {
		glog.Infof("failed to clone %s/%s, will try to cleanup: %s", sourceNS, sourcePVCName, err)
		sourcePVCRef = ""

		// Delete destDir, partial clone? Fallthrough to normal Mkdir().
		err = os.RemoveAll(destDir)
		if err != nil {
			glog.Errorf("failed to cleanup partially cloned %s/%s: %s", sourceNS, sourcePVCName, err)
			// TODO: try to move partial cloned contents to some "junk" dir and continue?
			return
		}

		err = os.MkdirAll(destDir, 0777)
		if err != nil {
			glog.Errorf("Failed to create subdir for new pvc %s: %s", options.PVC.Name, err)
			return
		}
	} else {
		glog.Infof("successfully cloned %s/%s for PVC %s/%s", sourceNS, sourcePVCName, pvc.Namespace, pvc.Name)
	}

	// based on the PVC we can get the PV
	supervolPV, err := p.client.CoreV1().PersistentVolumes().Get(supervolPVC.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("could not find PV for PVC %s/%s", supervolPVC.Namespace, supervolPVC.Name)
		return
	}

	// TODO: copied from Provision()

	// TODO: set quota? Not possible through standard tools, only gluster CLI?

	// subdir has been setup, create the PVC object
	ep, err := p.copyEndpoints(supervolPVC.Namespace, supervolPV, options.PVC.Namespace, options.PVC.Name)
	if err != nil {
		glog.Errorf("failed to copy endpoint from supervol %s: %s", supervolPV.Name, err)
		return
	}

	// TODO: glusterfile creates a Service for each PVC, is that really needed?

	glusterfs := &v1.GlusterfsVolumeSource{
		EndpointsName: ep.ObjectMeta.Name,
		Path:          p.makeMountPath(supervolPV, options.PVC.Namespace, options.PVC.UID),
		ReadOnly:      false,
	}

	mode := v1.PersistentVolumeFilesystem
	pvSpec := v1.PersistentVolumeSpec{
		PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
		AccessModes:                   options.PVC.Spec.AccessModes,
		MountOptions:                  supervolPV.Spec.MountOptions,
		VolumeMode:                    &mode,
		Capacity: v1.ResourceList{
			v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
		},
		PersistentVolumeSource: v1.PersistentVolumeSource{
			Glusterfs: glusterfs,
		},
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
			Annotations: map[string]string{
				gidAnn:         strconv.FormatInt(int64(*gid), 10),
				glusterTypeAnn: "subvol",
				parentPVCAnn:   supervolPVC.Namespace + "/" + supervolPVC.Name,
				"Description":  descAnn,
			},
		},
		Spec: pvSpec,
	}

	// sourcePVCRef will be empty if cloning failed
	if sourcePVCRef != "" {
		options.PVC.Annotations[CloneOfAnn] = sourcePVCRef
		err = p.annotatePVC(options.PVC.Namespace, options.PVC.Name, options.PVC.Annotations)
		if err != nil {
			// TODO: retry or cleanup the cloned data?
			glog.Errorf("cloning %s was successful, but setting the annotation on PVC %s/%s was not", sourcePVCRef, options.PVC.Namespace, options.PVC.Name)
		}
	}

	// TODO: create the new PV in the API server
	_, err = p.client.CoreV1().PersistentVolumes().Create(pv)
	if err != nil && errors.IsAlreadyExists(err) {
		glog.Infof("PersistentVolume %s already exist, that is ok", options.PVC.Name, options.PVC.Namespace)
	} else if err != nil {
		glog.Errorf("failed to create PersistentVolume %s: %s", options.PVName, err)
	}

	cloneStatus[cloneStatusAnn] = "Finished"
}

// Provision creates a storage asset and returns a PV object representing it.
func (p *glusterSubvolProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	// check if cloning is already initiated for this PVC
	if p.isCloning(options.PVC) {
		return nil, fmt.Errorf("PVC %s/%s is already getting provisioned", options.PVC.Namespace, options.PVC.Name)
	}

	var supervolNS, supervolPVCName string
	gidAllocate := true
	for k, v := range options.Parameters {
		switch strings.ToLower(k) {
		case "namespace":
			supervolNS = v
		case "pvc":
			supervolPVCName = v
		case "gidmin":
		// Let allocator handle
		case "gidmax":
		// Let allocator handle
		case "gidallocate":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("invalid value %s for parameter %s: %v", v, k, err)
			}
			gidAllocate = b
		}
	}

	if supervolPVCName == "" {
		return nil, fmt.Errorf("pvc is a required options for volume plugin %s", provisionerName)
	}

	// get the PVC that has been configured
	supervolPVC, err := p.getPVC(supervolNS, supervolPVCName)
	if err != nil {
		return nil, fmt.Errorf("could not find pvc %s in namespace %s", supervolPVCName, supervolNS)
	}

	err = p.validateProvisionRequirements(options.PVC, supervolPVC)
	if err != nil {
		return nil, err
	}

	// based on the PVC we can get the PV
	supervolPV, err := p.client.CoreV1().PersistentVolumes().Get(supervolPVC.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not find PV for PVC %s/%s", supervolNS, supervolPVCName)
	}

	// we only support Gluster Volumes for now
	if supervolPV.Spec.Glusterfs == nil {
		return nil, fmt.Errorf("the supervol PVC %s/%s needs to be of type Glusterfs", supervolNS, supervolPVCName)
	}

	// mount the supervolPV
	mountpoint, err := p.mountPV(supervolNS, supervolPV)
	if err != nil {
		return nil, fmt.Errorf("failed to mount PV %s: %s", supervolPV, err)
	}

	// verify that the supervol is large enough
	statfsBuf := &syscall.Statfs_t{}
	err = syscall.Statfs(mountpoint, statfsBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to get statfs for PV %s: %s", supervolPV, err)
	}

	subvolSize := options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	if uint64(subvolSize.Value()) > (statfsBuf.Bavail * uint64(statfsBuf.Bsize)) {
		return nil, fmt.Errorf("PVC request for %d bytes can not be fulfilled by PV %s", subvolSize.Value(), supervolPV)
	}

	// TODO: set quota once there is a real API for it, quotactl()?

	// in case smartcloning fails, the new PVC should be created, but
	// without the CloneOf annotation. The assumption is that the origin of
	// the request to clone can fallback to other cloning/copying in case
	// the CloneRequest annotation was ignored/unknown.

	// full path of the directory for the new PV
	destDir := p.makeSubvolPath(mountpoint, options.PVC.Namespace, options.PVC.UID)

	var gid *int
	if gidAllocate {
		var allocate int // suggested by vetshadow, warning for 'allocate, err := ..'
		allocate, err = p.allocator.AllocateNext(options)
		if err != nil {
			return nil, fmt.Errorf("allocator error: %v", err)
		}
		gid = &allocate
	}
	glog.Infof("Allocated GID %d for PVC %s", *gid, options.PVC.Name)

	// In case an error occurred during cloning, an empty PVC should be
	// created. This is the same behaviour as provisioners that do not
	// provide cloning support. The "k8s.io/CloneOf" annotation should only
	// get set if cloning was successful.
	isCloning, err := p.tryClone(options, supervolPVC, mountpoint, destDir, gid)
	if err != nil || !isCloning {
		if err != nil {
			glog.Errorf("failed to clone, creating empty PVC %s/%s: %s", options.PVC.Namespace, options.PVC.Name, err)
		}
		err = os.MkdirAll(destDir, 0777)
		if err != nil {
			return nil, fmt.Errorf("Failed to create subdir for new pvc %s: %s", options.PVC.Name, err)
		}
		glog.Infof("successfully created Gluster Subvol %+v with size and volID", destDir)
	}

	// when tryClone() started cloning in the background, return
	if isCloning {
		// an error should be returned, the controller panics otherwise
		return nil, fmt.Errorf("provisioning of PVC %s/%s continues in the background", options.PVC.Namespace, options.PVC.Name)
	}

	// TODO: set quota? Not possible through standard tools, only gluster CLI?

	// subdir has been setup, create the PVC object
	ep, err := p.copyEndpoints(supervolNS, supervolPV, options.PVC.Namespace, options.PVC.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to copy endpoint from supervol %s: %s", supervolPV.Name, err)
	}

	// TODO: glusterfile creates a Service for each PVC, is that really needed?

	glusterfs := &v1.GlusterfsVolumeSource{
		EndpointsName: ep.ObjectMeta.Name,
		Path:          p.makeMountPath(supervolPV, options.PVC.Namespace, options.PVC.UID),
		ReadOnly:      false,
	}

	mode := v1.PersistentVolumeFilesystem
	pvSpec := v1.PersistentVolumeSpec{
		PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
		AccessModes:                   options.PVC.Spec.AccessModes,
		MountOptions:                  supervolPV.Spec.MountOptions,
		VolumeMode:                    &mode,
		Capacity: v1.ResourceList{
			v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
		},
		PersistentVolumeSource: v1.PersistentVolumeSource{
			Glusterfs: glusterfs,
		},
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
			Annotations: map[string]string{
				gidAnn:         strconv.FormatInt(int64(*gid), 10),
				glusterTypeAnn: "subvol",
				parentPVCAnn:   supervolNS + "/" + supervolPVC.Name,
				"Description":  descAnn,
			},
		},
		Spec: pvSpec,
	}

	return pv, nil
}

func (p *glusterSubvolProvisioner) Delete(pv *v1.PersistentVolume) error {
	// we only support Gluster Volumes for now
	if pv.Spec.Glusterfs == nil {
		return fmt.Errorf("PV %s needs to be of type Glusterfs", pv.Name)
	}

	// return the uid/gid back to the pool
	err := p.allocator.Release(pv)
	if err != nil {
		return err
	}

	// TODO(test): need to delete the endpoint
	err = p.client.CoreV1().Endpoints(pv.Spec.ClaimRef.Namespace).Delete(pv.Spec.Glusterfs.EndpointsName, &metav1.DeleteOptions{})
	if err != nil {
		glog.Infof("could not delete endpoint %s/%s: %s", pv.Spec.ClaimRef.Namespace, pv.Spec.Glusterfs.EndpointsName, err)
	}

	// need to get the supervol where this PV lives
	parentPVC, ok := pv.ObjectMeta.Annotations[parentPVCAnn]
	if !ok {
		return fmt.Errorf("missing %s annotation in PV %s, can not delete the PV", parentPVC, pv.Name)
	}

	// the parentPVCAnn is in the format <namespace>/<pvc>
	parts := strings.Split(parentPVC, "/")
	if len(parts) != 2 {
		return fmt.Errorf("failed to parse annotation %s:%s for PV %s, can not delete the PV", parentPVCAnn, parentPVC, pv.Name)
	}

	supervolNS := parts[0]
	supervolPVC, err := p.getPVC(supervolNS, parts[1])
	if err != nil {
		return fmt.Errorf("failed to find supervol PVC %s: %s", parentPVC, err)
	}

	supervolPV, err := p.client.CoreV1().PersistentVolumes().Get(supervolPVC.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("could not find PV for PVC %s/%s, can not delete PV %s: %s", supervolNS, supervolPVC.Name, pv.Name, err)
	}

	// in case the provisioner restarted, the supervol needs to get mounted again
	mountpoint, err := p.mountPV(supervolNS, supervolPV)
	if err != nil {
		return fmt.Errorf("failed to mount supervol PVC %s: %s", parentPVC, err)
	}

	subvolPath := p.makeSubvolPath(mountpoint, pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.UID)

	glog.Infof("deleting volume, path %s", subvolPath)

	_, err = os.Stat(subvolPath)
	if err != nil {
		glog.Errorf("path %s for PV %s does not exist, marking deletion successful: %s", subvolPath, pv.Name, err)
	} else {
		err = os.RemoveAll(subvolPath)
		if err != nil {
			glog.Errorf("error when deleting PV %s: %s", pv.Name, err)
			return err
		}
	}
	glog.Infof("PV %s deleted successfully", pv.Name)

	return nil
}

var (
	master     = flag.String("master", "", "Master URL")
	kubeconfig = flag.String("kubeconfig", "", "Absolute path to the kubeconfig")
	id         = flag.String("id", "", "Unique provisioner identity")
)

func main() {
	flag.Parse()
	flag.Set("logtostderr", "true")

	// Create an InClusterConfig and use it to create a client for the controller
	// to use to communicate with Kubernetes

	var config *rest.Config
	var err error
	if *master != "" || *kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags(*master, *kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}

	if err != nil {
		glog.Fatalf("Failed to create config: %v", err)
	}

	provName := provisionerName
	provEnvName := os.Getenv(provisionerNameKey)

	// Precedence is given for ProvisionerNameKey
	if provEnvName != "" && *id != "" {
		provName = provEnvName
	}

	if provEnvName == "" && *id != "" {
		provName = *id
	}

	// Fetch the timeout for expiring supervol PV mounts
	timeout := 900 // 15 minutes by default
	timeoutEnv := os.Getenv(unmountDelay)
	if timeoutEnv != "" {
		timeout, err = strconv.Atoi(timeoutEnv)
		if err != nil {
			glog.Fatalf("invalid integer for %s (%s): %s", unmountDelay, timeoutEnv, err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Failed to create client:%v", err)
	}

	// The controller needs to know what the server version is because out-of-tree
	// provisioners aren't officially supported until 1.5
	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		glog.Fatalf("Error getting server version:%v", err)
	}

	// Create the provisioner
	glusterSubvolProvisioner, gsp, err := newGlusterSubvolProvisioner(clientset, provName, timeout)
	if err != nil {
		glog.Fatalf("Failed to instantiate the provisioned: %v", err)
	}

	// unmount all supervols when main() exits
	defer gsp.umountAll()

	// Start the provision controller
	pc := controller.NewProvisionController(
		clientset,
		provName,
		glusterSubvolProvisioner,
		serverVersion.GitVersion,
	)
	pc.Run(wait.NeverStop)
}
