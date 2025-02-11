#!/bin/bash
set -ex

WORKER=false
GPU=false
LOAD=false
ZFS_DIR=""
DOCKER_DIR=""

help () {
cat << EOF
Install and prepare the necessary dependencies on the local machine to run either an Ilúvatar worker or load generator.
A machine requires no special setup to run the Ilúvatar controller.
**Must be run as root.**
usage: ./setup.sh [--worker][--load][--gpu][--zfs-dir <DIR>][--docker-dir <DIR>][--help]

[--worker]: Install worker dependencies.
[--load]: Install load generator dependencies.
[--gpu]: Install Nvidia drivers and container toolkit.
[--zfs-dir <DIR>]: Prepare a zfs pool of 100 GB in DIR and configure containerd to use it.
[--docker-dir <DIR>]: Change the directory in which Docker stores data to DIR. Useful to redirect it to a large drive.
[--help]: Display this help information.
EOF
exit 1
}

for i in "$@"
do
case $i in
    --worker)
    WORKER=true
    ;;
    --load)
    LOAD=true
    ;;
    --gpu)
    GPU=true
    ;;
    --zfs-dir=*)
    ZFS_DIR="${i#*=}"
    ;;
    --docker-dir=*)
    DOCKER_DIR="${i#*=}"
    ;;
    -h|--help)
    help
    ;;
    *)
    # unknown option
    help
    ;;
esac
done

cmd_missing () {
  sh -c "$1" &> /dev/null
  if $? ; then
    return 1
  else
    return 0
  fi
}

# basics
apt-get update -y && apt-get upgrade -y

if [ "$LOAD" = "true" ]; then
  apt-get install -y curl jq protobuf-compiler # "linux-headers-$(uname -r)"

  # rust
  if cmd_missing "cargo"; then
    curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh -s -- -y
    . "$HOME/.cargo/env"
  fi
  if cmd_missing "cross"; then
    cargo install cross --git https://github.com/cross-rs/cross
  fi

  # python
  apt-get install -y python3-pip
  python3 -m pip install ansible numpy matplotlib pandas paramiko scp
fi

if [ "$WORKER" = "true" ]; then
apt-get install -y wget curl runc bridge-utils iproute2 iptables net-tools sysstat jq protobuf-compiler # "linux-headers-$(uname -r)"
fi

# docker 

## Add Docker's official GPG key:
if cmd_missing "docker ps"; then
apt-get install ca-certificates curl apt-transport-https -y
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc

## Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
if [ -n "$USER" ]; then
  usermod -aG docker "$USER"
fi
newgrp docker

# large docker file dir
if [ -n "$DOCKER_DIR" ]; then
daemon_file="/etc/docker/daemon.json"
if [ -f $daemon_file ]; then
  jq ".\"data-root\" = \"$DOCKER_DIR\"" $daemon_file > tmp.json
  mv tmp.json $daemon_file
else
  echo "{ \"data-root\":\"$DOCKER_DIR\" }" | tee $daemon_file
fi
systemctl restart docker
fi
fi

if [ "$WORKER" = "true" ]; then
# CNI
ARCH=amd64
GO_VERSION=$(curl -s https://go.dev/dl/?mode=json | jq -r '.[0].version')
tar="${GO_VERSION}.linux-${ARCH}.tar.gz"

wget https://go.dev/dl/${tar}
rm -rf /usr/local/go/
tar -C /usr/local -xzf ${tar}
rm ${tar}
export PATH=$PATH:/usr/local/go/bin

go install github.com/containernetworking/cni/cnitool@latest
gopth=$(go env GOPATH)
mkdir -p /opt/cni/bin
mv ${gopth}/bin/cnitool /opt/cni/bin

ARCH=amd64
CNI_VERSION=$(curl -s https://api.github.com/repos/containernetworking/plugins/releases/latest |   jq --raw-output '.tag_name')

curl -sSL https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz | tar -xz -C /opt/cni/bin
fi

# zfs
if [ -n "$ZFS_DIR" ] && [ "$WORKER" = "true" ]; then
  apt-get install -y zfsutils-linux

  mkdir -p $ZFS_DIR/zfs
  fallocate -l 100G $ZFS_DIR/zfs/ilu-pool
  # optionally this can be created using whole devices, not a file
  zpool create ilu-pool $ZFS_DIR/zfs/ilu-pool
  zfs create -o mountpoint=/var/lib/containerd/io.containerd.snapshotter.v1.zfs ilu-pool/containerd
  systemctl restart containerd
fi


# nvidia driver & cuda
if [ "$GPU" = "true" ] && [ "$WORKER" = "true" ]; then
  apt install -y nvidia-headless-470-server nvidia-utils-470-server nvidia-compute-utils-470-server

  distro=ubuntu2204
  arch=x86_64
  wget https://developer.download.nvidia.com/compute/cuda/repos/$distro/$arch/cuda-archive-keyring.gpg
  mv cuda-archive-keyring.gpg /usr/share/keyrings/cuda-archive-keyring.gpg
  apt update
  apt-get install -y cuda-toolkit

  # nvidia docker
  curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
    && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
      sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
      tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
  apt-get update
  apt-get install -y nvidia-container-toolkit
  nvidia-ctk runtime configure --runtime=docker
  systemctl restart docker

  # daemon
  echo "[Unit]
  Description=NVIDIA Persistence Daemon
  Wants=syslog.target
  StopWhenUnneeded=true
  Before=systemd-backlight@backlight:nvidia_0.service

  [Service]
  Type=forking
  ExecStart=/usr/bin/nvidia-persistenced --user nvidia-persistenced --persistence-mode --verbose
  ExecStopPost=/bin/rm -rf /var/run/nvidia-persistenced" | tee /lib/systemd/system/nvidia-persistenced.service
  systemctl daemon-reload
  systemctl restart nvidia-persistenced
  # reboot gpu machine
  echo "You will need to reboot after this is completed"
fi

if [ "$WORKER" = "true" ]; then
# file limits
sh -c "cat >> /etc/security/limits.conf" << EOF
root            soft    nofile          1000000
root            hard    nofile          1000000
root            soft    nproc           1000000
root            hard    nproc           1000000
*            soft    nofile          1000000
*            hard    nofile          1000000
*            soft    nproc           1000000
*            hard    nproc           1000000
EOF
# reboot machine
echo "You will need to reboot after this is completed"
fi

