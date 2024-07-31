#!/bin/sh

# This script prepares dependencies and configuration to run the samples on the landing page and in this directory.
# It is expecting to be run in the same directory as it is located.

if ! [ -x "$(command -v go)" ];
then
  echo "go not found, installing"
  ARCH=amd64
  GO_VERSION=1.22.5
  tar="go${GO_VERSION}.linux-${ARCH}.tar.gz"

  wget https://go.dev/dl/${tar}
  sudo rm -rf /usr/local/go/
  sudo tar -C /usr/local -xzf ${tar}
  rm ${tar}
  export PATH="$PATH:/usr/local/go/bin"
fi

go install github.com/containernetworking/cni/cnitool@latest
gopth=$(go env GOPATH)
sudo mkdir -p /opt/cni/bin
sudo mv ${gopth}/bin/cnitool /opt/cni/bin

ARCH=amd64
CNI_VERSION=v1.1.1

curl -sSL https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz | sudo tar -xz -C /opt/cni/bin

sudo apt install -y jq ensurepip
python3 -m pip install virtualenv
python3 -m venv --clear examples-venv
source ./examples-venv/bin/activate
python3 -m pip install --upgrade pip --no-warn-script-location
python3 -m pip install ansible numpy pandas matplotlib --no-warn-script-location
deactivate

name=$(ip route get 8.8.8.8 | awk '{ print $5; exit }')

local_json="../../iluvatar_worker/src/worker.dev.json"
cp ../../iluvatar_worker/src/worker.json $local_json
jq ".networking.hardware_interface = \"$name\"" $local_json > tmp.json && mv tmp.json $local_json
jq ".container_resources.snapshotter = \"overlayfs\"" $local_json > tmp.json && mv tmp.json $local_json
jq ".influx.enabled = false" $local_json > tmp.json && mv tmp.json $local_json

local_json="../../iluvatar_controller/src/controller.dev.json"
cp ../../iluvatar_controller/src/controller.json $local_json
jq ".influx.enabled = false" $local_json > tmp.json && mv tmp.json $local_json

cat <<EOT > ../../ansible/group_vars/local_addresses.yml
servers:
  localhost:
    internal_ip: 127.0.0.1
    ipmi_ip: 127.0.0.1
    hardware_interface: "$name"
  127.0.0.1:
    internal_ip: 127.0.0.1
    ipmi_ip: 127.0.0.1
    hardware_interface: "$name"
EOT

ILU_HOME="../.."
ret=$(pwd)
cd $ILU_HOME
make release
cd $ret