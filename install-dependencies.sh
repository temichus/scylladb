#!/bin/bash

. /etc/os-release

fedora_packages=(
    python3-virtualenv
    python3-devel
    python3-pyyaml
    python3-six
    python3-requests
    python3-psutil
    python3-cassandra-driver python-cassandra-driver-doc
)

if [ "$ID" = "fedora" ]; then
    dnf install -y "${fedora_packages[@]}"
fi
