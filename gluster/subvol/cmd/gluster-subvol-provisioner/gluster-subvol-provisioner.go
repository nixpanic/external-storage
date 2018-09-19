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
	"strconv"
	"strings"

	"github.com/containerd/continuity/fs"
	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/lib/controller"
	"github.com/kubernetes-incubator/external-storage/lib/gidallocator"
	"github.com/kubernetes-incubator/external-storage/lib/util"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

const (
	provisionerName           = "gluster.org/gluster-subvol"
	provisionerNameKey        = "PROVISIONER_NAME"
	descAnn                   = "Gluster-subvol: Dynamically provisioned PV"
	dynamicEpSvcPrefix        = "gluster-subvol-dynamic-"
	glusterTypeAnn            = "gluster.org/type"
	gidAnn                    = "pv.beta.kubernetes.io/gid"

	// CloneRequestAnn is an annotation to request that the PVC be provisioned as a clone of the referenced PVC
	CloneRequestAnn = "k8s.io/CloneRequest"

	// CloneOfAnn is an annotation to indicate that a PVC is a clone of the referenced PVC
	CloneOfAnn = "k8s.io/CloneOf"
)

type glusterSubvolProvisioner struct {
	client   kubernetes.Interface
	identity string
	provisionerConfig
	allocator gidallocator.Allocator
	options   controller.VolumeOptions
}

type provisionerConfig struct {
	// the supervol is used to get details for mounting
	pvc             *v1.PersistentVolumeClaim
	pv              *v1.PersistentVolume
	mountpoint      string
	subvolPrefix    string
	gidMin          int
	gidMax          int
}

var _ controller.Provisioner = &glusterSubvolProvisioner{}

var accessModes = []v1.PersistentVolumeAccessMode{
	v1.ReadWriteMany,
	v1.ReadOnlyMany,
	v1.ReadWriteOnce,
}


func newGlusterSubvolProvisioner(client kubernetes.Interface, id string) (controller.Provisioner, error) {
	p := &glusterSubvolProvisioner{
		client:    client,
		identity:  id,
		allocator: gidallocator.New(client),
	}

	err := p.parseClassParameters()
	if err != nil {
		return nil, err
	}

	return p, nil
}


func (p *glusterSubvolProvisioner) getPVC(ns string, name string) (*v1.PersistentVolumeClaim, error) {
	return p.client.CoreV1().PersistentVolumeClaims(ns).Get(name, metav1.GetOptions{})
}


func (p *glusterSubvolProvisioner) annotatePVC(ns string, name string, updates map[string]string) error {
	// Retrieve the latest version of PVC before attempting update
	// RetryOnConflict uses exponential backoff to avoid exhausting the apiserver
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, getErr := p.getPVC(ns, name)
		if getErr != nil {
			panic(fmt.Errorf("Failed to get latest version of PVC: %v", getErr))
		}

		for k, v := range updates {
			result.Annotations[k] = v
		}
		_, updateErr := p.client.CoreV1().PersistentVolumeClaims(ns).Update(result)
		return updateErr
	})
	if retryErr != nil {
		glog.Errorf("Update failed: %v", retryErr)
		return retryErr
	}
	return nil
}


// Provision creates a storage asset and returns a PV object representing it.
func (p *glusterSubvolProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	if options.PVC.Spec.Selector != nil {
		return nil, fmt.Errorf("claim Selector is not supported")
	}

	if !util.AccessModesContainedInAll(accessModes, options.PVC.Spec.AccessModes) {
		return nil, fmt.Errorf("invalid AccessModes %v: only AccessModes %v are supported", options.PVC.Spec.AccessModes, accessModes)
	}

	glog.V(1).Infof("VolumeOptions %v", options)
	gidAllocate := true
	for k, v := range options.Parameters {
		switch strings.ToLower(k) {
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

	var gid *int
	if gidAllocate {
		allocate, err := p.allocator.AllocateNext(options)
		if err != nil {
			return nil, fmt.Errorf("allocator error: %v", err)
		}
		gid = &allocate
	}
	glog.V(1).Infof("Allocated GID %d for PVC %s", *gid, options.PVC.Name)


	/* TODO:
	 * - set quota for the size
	 * - verify that the supervol is large enough
	volSize := options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	volSizeBytes := volSize.Value()
	volszInt := int(util.RoundUpToGiB(volSizeBytes))
	*/


	// in case smartcloning fails, the new PVC should be created, but
	// without the CloneOf annotation. The assumption is that the origin of
	// the request to clone can fallback to other cloning/copying in case
	// the CloneRequest annotation was ignored/unknown.

	destDir := p.mountpoint+"/"+options.PVC.Namespace+"/"+options.PVC.Name

	// sourcePVCRef points to the PVC that should get cloned
	// is sourcePVCRef is (still) set, a CloneOf annotation in the new PVC will be added
	sourcePVCRef, ok := options.PVC.Annotations[CloneRequestAnn]
	if ok && sourcePVCRef != "" {
		// sourcePVCRef is like (namespace/)?pvc
		var sourceNS, sourcePVCName string

		parts := strings.Split("/", sourcePVCRef)
		if len(parts) == 1 {
			sourceNS = options.PVC.Namespace
			sourcePVCName = parts[0]
		} else if len(parts) == 2 {
			sourceNS = parts[0]
			sourcePVCName = parts[1]
		} else {
			return nil, fmt.Errorf("failed to parse namespace/pvc from %s", sourcePVCRef)
		}

		// TODO: sourcePVCRef needs to be on the same Gluster volume

		// TODO: check if the size of the PVC >= source

		// TODO: where is the CloneOf mounted? Add to sourceDir/destDir
		sourcePVC, err := p.getPVC(sourcePVCName, sourceNS)
		if err != nil {
			return nil, fmt.Errorf("failed to find PVC %s/%s:", sourceNS, sourcePVCName, err)
		}

		// TODO: get the sourcePV from the sourcePVC
		sourcePV, err := p.client.CoreV1().PersistentVolumes().Get(sourcePVC.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("could not find PV %s for PVC %s", sourcePVC.Spec.VolumeName, sourcePVCName)
		}

		// TODO: how to find out the type of the PV?
		sourceDir := p.mountpoint+"/"+sourcePV.Spec.Glusterfs.Path

		// verify that the sourcePVC is on the p.pv
		st, err := os.Stat(sourceDir)
		if err != nil || !st.Mode().IsDir() {
			return nil, fmt.Errorf("failed to clone %s, path does not exist on PVC %s", sourcePVCRef, p.pvc.Name)
		}

		// verification has been done!

		// optimized copy, uses copy_file_range() if possible
		err = fs.CopyDir(destDir, sourceDir)
		if err != nil {
			glog.V(1).Infof("failed to clone %s/%s, will try to cleanup: %s", sourceNS, sourcePVCName, err)
			sourcePVCRef = ""

			// Delete destDir, partial clone? Fallthrough to normal Mkdir().
			err = os.RemoveAll(destDir)
			if err != nil {
				return nil, fmt.Errorf("failed to cleanup partially cloned %s/%s: %s", sourceNS, sourcePVCName, err)
			}
		}
	}

	// No CloneRequest annotation, or cloning failed. Create a new empty subdir.
	if sourcePVCRef == "" {
		err := os.Mkdir(destDir, 0770)
		// TODO: handle error
		if err != nil {
			return nil, fmt.Errorf("Failed to create subdir for new pvc %v: ", options.PVC.Name, err)
		}
		glog.V(1).Infof("successfully created Gluster Subvol %+v with size and volID", destDir)
	}

	// TODO: need to os.Chgrp() w/ new gid for all files? Should be done by mount process.
	// TODO: set quota? Not possible through standard tools, only gluster CLI?

	glusterfs := &v1.GlusterfsVolumeSource{
		// TODO: we'll reuse the existing endpoint for now, needs to be
		// one in the right namespace, use: ep := Endpoints.DeepCopy()
		EndpointsName: p.pv.Spec.Glusterfs.EndpointsName,
		Path:          p.pv.Spec.Glusterfs.Path+"/"+options.PVC.Namespace+"/"+options.PVC.Name,
		ReadOnly:      false,
	}

	mode := v1.PersistentVolumeFilesystem
	pvc := v1.PersistentVolumeSpec{
		PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
		AccessModes:                   options.PVC.Spec.AccessModes,
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
				gidAnn:                   strconv.FormatInt(int64(*gid), 10),
				glusterTypeAnn:           "subvol",
				"Description":            descAnn,
				v1.MountOptionAnnotation: "", // TODO: get mount options from PVC
			},
		},
		Spec: pvc,
	}

	// sourcePVCRef will be empty if there was no cloning request, or cloning failed
	if (sourcePVCRef != "") {
		pv.ObjectMeta.Annotations[CloneOfAnn] = sourcePVCRef
	}

	return pv, nil
}


func (p *glusterSubvolProvisioner) Delete(volume *v1.PersistentVolume) error {

	glog.V(1).Infof("deleting volume, path %s", volume.Spec.Glusterfs.Path)

	err := p.allocator.Release(volume)
	if err != nil {
		return err
	}

	err = os.RemoveAll(p.mountpoint+"/"+volume.Spec.Glusterfs.Path)
	if err != nil {
		glog.Errorf("error when deleting PV %s: %s", volume.Name, err)
		return err
	}
	glog.V(2).Infof("PV %s deleted successfully", volume.Name)

	// TODO: no need to delete the endpoint, it is shared with this provisioner (always?)
	return nil
}


// Create a provisionerConfig based on the parameters set in the StorageClass
func (p *glusterSubvolProvisioner) parseClassParameters() error {
	var err error
	var ns, pvc string

	for k, v := range p.options.Parameters {
		switch strings.ToLower(k) {
		case "namespace":
			ns = v
		case "pvc":
			pvc = v
		case "mountpoint":
			p.mountpoint = v
		case "subvolprefix":
			p.subvolPrefix = v
		case "gidmin":
		case "gidmax":
		case "smartclone":
		default:
			return fmt.Errorf("invalid option %q for volume plugin %s", k, provisionerName)
		}
	}

	if (len(pvc) == 0) {
		return fmt.Errorf("pvc is a required options for volume plugin %s", provisionerName)
	}

	// get the PVC that has been configured
	p.pvc, err = p.getPVC(ns, pvc)
	if err != nil {
		return fmt.Errorf("could not find pvc %s in namespace %s", pvc, ns)
	}

	// check permission mode, the PV should be RWX as it is mounted here,
	// and we'll create subdirs as new PVCs
	if !util.AccessModesContains(p.pvc.Spec.AccessModes, v1.ReadWriteMany) {
		return fmt.Errorf("this provisioner requires the PVC %s/%s to have ReadWriteMany permissions", ns, pvc)
	}

	// based on the PVC we can get the PV
	p.pv, err = p.client.CoreV1().PersistentVolumes().Get(p.pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("could not find pv %s", p.pvc.Spec.VolumeName)
	}

	// TODO: verify that the PV is of tyle 'glusterfs'

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
	glusterSubvolProvisioner, err := newGlusterSubvolProvisioner(clientset, provName)
	if err != nil {
		glog.Fatalf("Failed to instantiate the provisioned: %v", err)
	}

	// Start the provision controller
	pc := controller.NewProvisionController(
		clientset,
		provName,
		glusterSubvolProvisioner,
		serverVersion.GitVersion,
	)
	pc.Run(wait.NeverStop)
}
