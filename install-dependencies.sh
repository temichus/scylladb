#!/bin/bash

. /etc/os-release

fedora_packages=(
    virtualenv
    python-devel
)

if [ "$ID" = "fedora" ]; then
    dnf install -y "${fedora_packages[@]}"
fi
