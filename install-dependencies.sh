#!/bin/bash

. /etc/os-release

fedora_packages=(
    virtualenv
    python-devel
    python2-pyyaml python3-pyyaml
    python2-six python3-six
    python2-requests python3-requests
    python2-psutil python3-psutil
)

if [ "$ID" = "fedora" ]; then
    dnf install -y "${fedora_packages[@]}"
fi
