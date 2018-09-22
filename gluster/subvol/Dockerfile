FROM centos:latest

LABEL maintainer="Niels de Vos <ndevos@redhat,com>"

# install glusterfs-fuse to make mounting possible
RUN yum -y install centos-release-gluster && yum -y install glusterfs-fuse && \
    yum --enablerepo=* clean all && \
    true

ADD cmd/gluster-subvol-provisioner/gluster-subvol-provisioner /usr/local/sbin/gluster-subvol-provisioner

# mounting FUSE filesystems may require loading the fuse.ko kernel module
VOLUME /usr/lib/modules

ENTRYPOINT ["/usr/local/sbin/gluster-subvol-provisioner"]
