FROM scylladb/scylla-toolchain:fedora-34-20210902

RUN sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
RUN sudo dnf -y install redhat-rpm-config redhat-lsb-core python2 python3-devel rsyslog cyrus-sasl iproute docker-ce-cli iptables-services openssl-devel libffi-devel libev libev-devel

# set java 8 as default
RUN echo 2 | sudo update-alternatives --config java

ADD docker/etc /etc
ADD requirements.txt requirements.txt

RUN python3 -m pip install setuptools wheel
RUN python3 -m pip install -r requirements.txt --ignore-installed

RUN python3 -m pip install https://github.com/scylladb/scylla-ccm/archive/master.zip
RUN ccm create cas-tmp --vnodes -n 1 --version=3.11.3
RUN cp -a ~/.ccm /.ccm
RUN python3 -m pip uninstall -y ccm
