FROM scylladb/scylla-toolchain:fedora-29-20190603

RUN sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
RUN sudo dnf -y install redhat-rpm-config python3-devel rsyslog cyrus-sasl iproute docker-ce-cli

ADD docker/etc /etc
ADD requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

RUN ccm create cas-tmp --vnodes -n 1 --version=3.11.3
RUN cp -a ~/.ccm /.ccm
RUN pip3 uninstall -y ccm
