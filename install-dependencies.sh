#!/bin/bash

. /etc/os-release

fedora_packages=(
    virtualenv
    python-devel
    python2-pyyaml python3-pyyaml
)

if [ "$ID" = "fedora" ]; then
    dnf install -y "${fedora_packages[@]}"
fi
