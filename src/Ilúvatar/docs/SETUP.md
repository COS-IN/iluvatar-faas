# Ilúvatar Setup

## Linux Dependencies

```bash
apt-get update -y
apt-get install -y curl runc bridge-utils iptables zfsutils-linux cmake net-tools gcc g++ libssl-dev pkg-config linux-tools-common linux-tools-`uname -r` libprotobuf-dev protobuf-compiler

```

## Go

```bash
ARCH=amd64
GO_VERSION=1.18.3
tar="go${GO_VERSION}.linux-${ARCH}.tar.gz"

wget https://go.dev/dl/${tar}
rm -rf /usr/local/go/
tar -C /usr/local -xzf ${tar}
rm ${tar}

export PATH=$PATH:/usr/local/go/bin
```

## CNITool

```bash
go install github.com/containernetworking/cni/cnitool@latest
gopth=$(go env GOPATH)
mkdir -p /opt/cni/bin
mv ${gopth}/bin/cnitool /opt/cni/bin
```

## CNI tools

```bash
ARCH=amd64
CNI_VERSION=v1.1.1

mkdir -p /opt/cni/bin
curl -sSL https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz | tar -xz -C /opt/cni/bin
```

## Containerd

```bash
  export VER=1.6.4
  curl -sSL https://github.com/containerd/containerd/releases/download/v$VER/containerd-$VER-linux-amd64.tar.gz > /tmp/containerd.tar.gz \
    && tar -xvf /tmp/containerd.tar.gz -C /usr/local/bin/ --strip-components=1

  containerd -version
  systemctl enable containerd
  systemctl daemon-reload
  systemctl restart containerd
  ```

If `systemctl enable containerd` gives an error about masking such as "Failed to enable unit: Unit file /etc/systemd/system/containerd.service is masked."
Run these commands, then re-run the `systemctl` commands
```bash
wget https://raw.githubusercontent.com/containerd/containerd/main/containerd.service
mkdir -p /usr/local/lib/systemd/system/
mv containerd.service /usr/local/lib/systemd/system/containerd.service
```

## Container forwarding

```bash
/sbin/sysctl -w net.ipv4.conf.all.forwarding=1
echo "net.ipv4.conf.all.forwarding=1" | tee -a /etc/sysctl.conf
```

## ZFS and file system

```bash
ilu_base=/data2/ilúvatar
# vary these based on your setup
# make sure these make it to your config files
mkdir -p $ilu_base/azure
mkdir -p $ilu_base/zfs
mkdir -p $ilu_base/logs

fallocate -l 100G $ilu_base/zfs/ilu-pool
# optionally this can be created using whole devices, not a file
zpool create ilu-pool $ilu_base/zfs/ilu-pool
zfs create -o mountpoint=/var/lib/containerd/io.containerd.snapshotter.v1.zfs ilu-pool/containerd
systemctl restart containerd
```

## File limits

Add the following lines to `/etc/security/limits.conf` and reboot the machine
```sh
root            soft    nofile          1000000
root            hard    nofile          1000000
root            soft    nproc           1000000
root            hard    nproc           1000000
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
