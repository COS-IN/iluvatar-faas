# Ilúvatar Setup

## Worker Runtime dependencies

These steps are required on a system that is going to *run* a worker.

### Packages

```bash
sudo apt-get update -y
sudo apt-get install -y curl runc bridge-utils iptables zfsutils-linux net-tools sysstat jq
```

Optional dependencies.

```bash
sudo apt-get install -y ipmitool linux-tools-common linux-tools-`uname -r`
```

`ipmitool` - needed to get hardware energy usage from `ipmi`.
`linux-tools-common` - needed to get `perf` metrics for energy and processor.

### Isolation

The Ilúvatar worker can support one or multiple types of container isolation on the same host, so long as they are set up correctly.
It must have at least *one* available to be useful, naturally.

#### Docker

Follow the Docker installation guide [here](https://docs.docker.com/engine/install/#server).
This will work out-of-the-box.

#### Containerd

**CNI tool.**
These provide a lazy solution for performing the networking setup for Containerd containers.

Start by installing go if it isn't available.

```bash
ARCH=amd64
GO_VERSION=$(curl -s https://go.dev/dl/?mode=json | jq -r '.[0].version')
tar="${GO_VERSION}.linux-${ARCH}.tar.gz"

wget https://go.dev/dl/${tar}
sudo rm -rf /usr/local/go/
sudo tar -C /usr/local -xzf ${tar}
rm ${tar}
export PATH=$PATH:/usr/local/go/bin
```

Then install the actual tool.

```bash
go install github.com/containernetworking/cni/cnitool@latest
gopth=$(go env GOPATH)
sudo mkdir -p /opt/cni/bin
sudo mv ${gopth}/bin/cnitool /opt/cni/bin

ARCH=amd64
CNI_VERSION=$(curl -s https://api.github.com/repos/containernetworking/plugins/releases/latest |   jq --raw-output '.tag_name')

curl -sSL https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz | sudo tar -xz -C /opt/cni/bin
```

**Note**: you can remove Go after this step.

**Containerd.**
If you didn't install Docker earlier, then you will need to install Containerd manually with these commands.
To check if it's needed, run `containerd -version`.

```bash
export VER=$(curl -s https://api.github.com/repos/containerd/containerd/releases/latest |   jq --raw-output '.tag_name')
curl -sSL https://github.com/containerd/containerd/releases/download/v$VER/containerd-$VER-linux-amd64.tar.gz > /tmp/containerd.tar.gz \
  && sudo tar -xvf /tmp/containerd.tar.gz -C /usr/local/bin/ --strip-components=1

sudo systemctl enable containerd
sudo systemctl daemon-reload
sudo systemctl restart containerd
```

If `systemctl enable containerd` gives an error about masking such as `Failed to enable unit: Unit file /etc/systemd/system/containerd.service is masked.`
Run these commands, then re-run the `systemctl` commands.

```bash
wget https://raw.githubusercontent.com/containerd/containerd/main/containerd.service
sudo mkdir -p /usr/local/lib/systemd/system/
sudo mv containerd.service /usr/local/lib/systemd/system/containerd.service
sudo rm -f /etc/systemd/system/containerd.service
sudo ln /usr/local/lib/systemd/system/containerd.service /etc/systemd/system/containerd.service
```

**ZFS and file system.**
Containerd supports a [variety of different snapshotters](https://github.com/containerd/containerd/tree/main/docs/snapshotters).
After initially using the default `overlayfs`, we chose to focus on using the `ZFS` snapshotter.
You are welcome to choose any supported one, simply set it up accordingly and specify the name in the worker configuration file.
These instructions are to set up a ZFS pool for use with Ilúvatar.

```bash
# vary these based on your setup
ilu_base=/data2/iluvatar
sudo mkdir -p $ilu_base/zfs

sudo fallocate -l 100G $ilu_base/zfs/ilu-pool
# optionally this can be created using whole devices, not a file
sudo zpool create ilu-pool $ilu_base/zfs/ilu-pool
sudo zfs create -o mountpoint=/var/lib/containerd/io.containerd.snapshotter.v1.zfs ilu-pool/containerd
sudo systemctl restart containerd
```

**File limits.**
Add the following lines to `/etc/security/limits.conf` and reboot the machine.

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

### GPU support

The machine will require recent NVIDIA drivers. We have testesd with machines running driver version 470.161.03 and CUDA Version: 11.4.
A significant shift, particularly a downgrade may result in failures for the applications running inside of containers.
But this cannot be known without live testing.

These steps need to be followed to enable NVIDIA GPU support for Ilúvatar containers.

Install NVIDIA Container Toolkit for Docker & containerd extensions.

* [Docker setup](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html#docker)
* [Containerd setup](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html#id6)

Create a service to run [nvidia-persistenced](https://docs.nvidia.com/deploy/driver-persistence/index.html#persistence-daemon) on the machine, providing lower GPU context start times and faster container booting.
This will live across potential system restarts.

```bash
echo "[Unit]
Description=NVIDIA Persistence Daemon
Wants=syslog.target
StopWhenUnneeded=true
Before=systemd-backlight@backlight:nvidia_0.service

[Service]
Type=forking
ExecStart=/usr/bin/nvidia-persistenced --user nvidia-persistenced --persistence-mode --verbose
ExecStopPost=/bin/rm -rf /var/run/nvidia-persistenced" | sudo tee /lib/systemd/system/nvidia-persistenced.service
sudo systemctl daemon-reload
sudo systemctl restart nvidia-persistenced
sudo systemctl status nvidia-persistenced
```

Success can be verified with this command:

```bash
nvidia-smi --format=csv,noheader --query-gpu=uuid,persistence_mode
```

## Build Setup

These steps are required on a system that is going to *build and orchestrate* a worker or cluster.

Install the build dependencies.
`cross-rs` requires `Docker` or `Podman`, see [here for more](https://github.com/cross-rs/cross?tab=readme-ov-file#dependencies).
<!---
These depencendies are no longer necessary when using cross-rs to build. Only docker is needed
sudo apt-get install -y cmake gcc g++ libssl-dev pkg-config libprotobuf-dev
-->

```bash
cargo install cross --git https://github.com/cross-rs/cross
python3 -m pip install ansible
```

Follow the instructions [here](https://www.rust-lang.org/tools/install) to install Rust.

Build the solution with `make`.

Create a `worker.dev.json` with your [worker config](./WORKER.md#configuration) and `controller.dev.json` with your [controller config](./CONTROLLER.md#configuration), if you are setting up locally.
Once your setup is ready, go try running a [sample function](./FUNCTIONS.md).
