#
## Create virtualenv
### Ubuntu

Java version 8 - https://linuxize.com/post/install-java-on-ubuntu-18-04/

```
sudo apt install openjdk-8-jdk
sudo update-alternatives --config java

sudo apt install python-pip python-virtualenv
```
### Debian

```bash
sudo dnf install Cython python3-psutil python3-yaml libev libev-devel ant-junit
```
### Centos / Redhat

```bash
yum install java-1.8.0-openjdk
```

## Download Relocatable

## Create Fork

Create a fork through github

## Clone Dtest

```bash
git clone git@github.com:`whoami`/scylla-dtest.git
git remote add upstream git@github.com:scylladb/scylla-dtest.git
git checkout manager_hackaton
cd scylla-dtest

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
pip install -e git+https://github.com/scylladb/scylla-ccm.git

ccm create <CLUSTER_NAME> --scylla --version='unstable/master:<NUM>'

```
