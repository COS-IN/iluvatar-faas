#!bin/bash

apt-get update -y
apt-get install -y curl runc bridge-utils iptables zfsutils-linux cmake gcc g++ libssl-dev pkg-config linux-tools-common linux-tools-`uname -r`

### Install go ###
ARCH=amd64
GO_VERSION=1.18.3

if [[ $(go version) != 0 ]]; then

  tar="go${GO_VERSION}.linux-${ARCH}.tar.gz"

  wget https://go.dev/dl/${tar}
  rm -rf /usr/local/go/
  tar -C /usr/local -xzf ${tar}
  rm ${tar}

  export PATH=$PATH:/usr/local/go/bin

fi

### Install cnitool ###
go install github.com/containernetworking/cni/cnitool@latest
gopth=$(go env GOPATH)
mkdir -p /opt/cni/bin
mv ${gopth}/bin/cnitool /opt/cni/bin


### Install cni tools ###
ARCH=amd64
CNI_VERSION=v1.1.1

mkdir -p /opt/cni/bin
curl -sSL https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz | tar -xz -C /opt/cni/bin


### Install containerd ###

if [[ $(ctr --version) != 0 ]]; then

  export VER=1.6.4
  curl -sSL https://github.com/containerd/containerd/releases/download/v$VER/containerd-$VER-linux-amd64.tar.gz > /tmp/containerd.tar.gz \
    && tar -xvf /tmp/containerd.tar.gz -C /usr/local/bin/ --strip-components=1

  containerd -version
  systemctl enable containerd
  systemctl daemon-reload
  systemctl restart containerd

fi

### Enable container forwarding
/sbin/sysctl -w net.ipv4.conf.all.forwarding=1
echo "net.ipv4.conf.all.forwarding=1" | tee -a /etc/sysctl.conf


### Set up zfs for containerd

fallocate -l 10G /zfs-ilu-pool
# optionally this can be created using whole devices, not a file
zpool create ilu-pool /zfs-ilu-pool
zfs create -o mountpoint=/var/lib/containerd/io.containerd.snapshotter.v1.zfs ilu-pool/containerd
systemctl restart containerd

### Install rust ###

if [[ $(cargo --version) != 0 ]]; then

  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

fi

cargo build -j $(nproc --all)
