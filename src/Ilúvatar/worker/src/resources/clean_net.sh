#!/bin/bash

echo "removing bridge"
# TODO: find out what all the bridge tool is doing
# remove it properly without hardcoding
sudo NETCONFPATH=/tmp/ilúvatar_worker/ CNI_PATH=/opt/cni/bin /home/alex/.gopth/bin/cnitool del il_worker_br /run/netns/mk_bridge_throwaway
sudo ip link delete IlWorkBr0 type bridge

echo "removing net namespaces"

ip -all netns delete