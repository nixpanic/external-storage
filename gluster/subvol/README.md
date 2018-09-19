# Gluster Subvol provisioner

## Summary

Use a Gluster Volume with and expose subdirectories as PVCs. Support for
simplified cloning (based on annotations) is one of the main features.


## Advantages

- no Gluster management operations
  - resilent, filesystem operations still function when Gluster servers are down
  - fast, filesystem operations only
- can support 'SmartCloning' on filesystem level (using reflink, `copy_file_range()`)
- thin provisioning is automatic


## Disadvantages (or TODO)

- no quota support yet, a PVC is not bounded by its requested size
- one Gluster Volume per StorageClass, might get many more StorageClasses
- resizing the Gluster Volume on-demand is possible, but not implemented

