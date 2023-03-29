# Ilúvatar Setup

## Linux Dependencies

```bash
sudo apt-get update -y
sudo apt-get install -y curl runc bridge-utils iptables zfsutils-linux cmake net-tools gcc g++ libssl-dev pkg-config linux-tools-common linux-tools-`uname -r` libprotobuf-dev protobuf-compiler sysstat
```

Optional dependencies
```
sudo apt-get install -y ipmitool
```

## Go

```bash
ARCH=amd64
GO_VERSION=1.18.3
tar="go${GO_VERSION}.linux-${ARCH}.tar.gz"

wget https://go.dev/dl/${tar}
sudo rm -rf /usr/local/go/
sudo tar -C /usr/local -xzf ${tar}
rm ${tar}

export PATH=$PATH:/usr/local/go/bin
```

## CNITool

```bash
go install github.com/containernetworking/cni/cnitool
gopth=$(go env GOPATH)
mkdir -p /opt/cni/bin
mv ${gopth}/bin/cnitool /opt/cni/bin
```

## CNI tools

```bash
ARCH=amd64
CNI_VERSION=v1.1.1

mkdir -p /opt/cni/bin
curl -sSL https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz | sudo tar -xz -C /opt/cni/bin
```

## Containerd

```bash
export VER=1.6.4
curl -sSL https://github.com/containerd/containerd/releases/download/v$VER/containerd-$VER-linux-amd64.tar.gz > /tmp/containerd.tar.gz \
  && sudo tar -xvf /tmp/containerd.tar.gz -C /usr/local/bin/ --strip-components=1

containerd -version
sudo systemctl enable containerd
sudo systemctl daemon-reload
sudo systemctl restart containerd
```

If `systemctl enable containerd` gives an error about masking such as "Failed to enable unit: Unit file /etc/systemd/system/containerd.service is masked."
Run these commands, then re-run the `systemctl` commands
```bash
wget https://raw.githubusercontent.com/containerd/containerd/main/containerd.service
sudo mkdir -p /usr/local/lib/systemd/system/
sudo mv containerd.service /usr/local/lib/systemd/system/containerd.service
```

## GPU support

The machine will require recent NVIDIA drivers. We have testest with machines running driver version 470.161.03 and CUDA Version: 11.4.
A significant shift, particularly a downgrade may result in failures for the applications running inside of containers.
But this cannot be known without live testing.

These steps need to be followed to enable NVIDIA GPU support for Ilúvatar containers.

Install NVIDIA Container Toolkit for Docker & containerd extensions.
* https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html#docker
* https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html#id6

Create a service to run [nvidia-persistenced](https://docs.nvidia.com/deploy/driver-persistence/index.html#persistence-daemon) on the machine, providing lower GPU context start times and faster container booting.
This will live across potential system restarts.
```
echo "[Unit]
Description=NVIDIA Persistence Daemon
Wants=syslog.target
StopWhenUnneeded=true
Before=systemd-backlight@backlight:nvidia_0.service

[Service]
Type=forking
ExecStart=/usr/bin/nvidia-persistenced --user nvidia-persistenced --persistence-mode --verbose
ExecStopPost=/bin/rm -rf /var/run/nvidia-persistenced" > /lib/systemd/system/nvidia-persistenced.service
systemctl restart nvidia-persistenced
systemctl status nvidia-persistenced
```

Success can be verified with this command:
```sh
nvidia-smi --format=csv,noheader --query-gpu=uuid,persistence_mode
```

## Container forwarding

```bash
sudo /sbin/sysctl -w net.ipv4.conf.all.forwarding=1
echo "net.ipv4.conf.all.forwarding=1" | sudo tee -a /etc/sysctl.conf
```

## ZFS and file system

Containerd supports a [variety of different snapshotters](https://github.com/containerd/containerd/tree/main/docs/snapshotters).
After initially using the default `overlayfs`, we chose to focus on using the `ZFS` snapshotter.
You are welcome to choose any supported one, simply set it up accordingly and specify the name in the worker configuration file.
These instructions are to set up a ZFS pool for use with Ilúvatar.

```bash
ilu_base=/data2/ilúvatar
# vary these based on your setup
# make sure these make it to your config files
sudo mkdir -p $ilu_base/azure
sudo mkdir -p $ilu_base/zfs
sudo mkdir -p $ilu_base/logs

sudo fallocate -l 100G $ilu_base/zfs/ilu-pool
# optionally this can be created using whole devices, not a file
sudo zpool create ilu-pool $ilu_base/zfs/ilu-pool
sudo zfs create -o mountpoint=/var/lib/containerd/io.containerd.snapshotter.v1.zfs ilu-pool/containerd
sudo systemctl restart containerd
```

## File limits

Add the following lines to `/etc/security/limits.conf` and reboot the machine
```sh
root            soft    nofile          1000000
root            hard    nofile          1000000
root            soft    nproc           1000000
root            hard    nproc           1000000
*            soft    nofile          1000000
*            hard    nofile          1000000
*            soft    nproc           1000000
*            hard    nproc           1000000
```

## Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Config

Create a `worker.dev.json` and `controller.dev.json` if you are setting up locally.
See [config](CONFIG.md) for details on how this can be done.

## Build solution

```bash
cargo build -j $(nproc --all)
```
