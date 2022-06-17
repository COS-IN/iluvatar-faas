#!/bin/bash
# ip netns delete mk_bridge_throwaway
# ip link delete IlWorkBr0 type bridge
# TODO: finish this, or put in rust?

echo "removing net namespaces"

sudo ip -all netns delete