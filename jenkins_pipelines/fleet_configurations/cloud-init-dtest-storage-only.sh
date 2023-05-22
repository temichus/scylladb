#!/usr/bin/env bash
set -x
set -o errexit
set -o pipefail
set -o nounset

num=$(fdisk -l | grep "^Disk /dev/nvme[1-9]" | wc -l)
for (( c=1; c<=$num; c++)) ; do
  echo -e "o\nn\np\n1\n\n\nt\n8e\nw" | fdisk `fdisk -l | grep "^Disk /dev/nvme[1-9]" | awk NR==$c | awk '{print $2}' | tr -d ':'`
done

part_list=$(fdisk -l | grep "Linux LVM" | awk '{print $1}' ORS=' ')
pvcreate -y $part_list
vgcreate -y vg_jenkins $part_list
lvcreate -y -n vol_data -l 100%FREE vg_jenkins
mkfs.xfs /dev/vg_jenkins/vol_data
mount /dev/vg_jenkins/vol_data /jenkins
chown jenkins:jenkins -R /jenkins
mkdir -p /jenkins/var/cache/ccache
mkdir -p /jenkins/tmp
mount --bind /jenkins/var/cache/ccache/ /var/cache/ccache
mount --bind /jenkins/tmp /tmp
chmod 1777 /tmp
echo "max_size = 95G" > /var/cache/ccache/ccache.conf
chown jenkins:ccache -R /jenkins/var/cache/ccache
