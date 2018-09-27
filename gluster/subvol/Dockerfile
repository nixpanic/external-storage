#
# Build a CentOS based gluster-subvol-provisioner container. The tools needed
# for the provisioner (mounting Gluster over FUSE) need to be included.
#
# While building the container, the context of the Dockerfile should be set to
# the root of the external-storage repository. The buildah command therefore
# looks like this:
#
# $ buildah bud --network=host --tag=gluster-subvol-provisioner:latest --file gluster/subvol/Dockerfile .
#

# use Fedora for building, it has godep
FROM fedora:latest AS build

LABEL maintainer="Niels de Vos <ndevos@redhat,com>"

# copy the checked-out branch/repository
COPY Gopkg.toml /srv/build/src/github.com/kubernetes-incubator/external-storage/
COPY lib /srv/build/src/github.com/kubernetes-incubator/external-storage/lib/
COPY gluster/subvol /srv/build/src/github.com/kubernetes-incubator/external-storage/gluster/subvol/

ENV GOPATH=/srv/build

RUN dnf -y install golang dep && \
    dnf -y update && \
    cd /srv/build/src/github.com/kubernetes-incubator/external-storage && \
    dep ensure -v && \
    cd gluster/subvol && \
    cd cmd/gluster-subvol-provisioner && \
    go build && \
    true

# runtime container
FROM centos:latest

LABEL maintainer="Niels de Vos <ndevos@redhat,com>"

# install glusterfs-fuse to make mounting possible
RUN yum -y install centos-release-gluster && yum -y install glusterfs-fuse && \
    yum -y update && \
    yum --enablerepo=* clean all && \
    true

COPY --from=build /srv/build/src/github.com/kubernetes-incubator/external-storage/gluster/subvol/cmd/gluster-subvol-provisioner/gluster-subvol-provisioner /usr/local/sbin/gluster-subvol-provisioner

# mounting FUSE filesystems may require loading the fuse.ko kernel module
VOLUME /usr/lib/modules

ENTRYPOINT ["/usr/local/sbin/gluster-subvol-provisioner"]
