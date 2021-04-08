FROM scylladb/scylla-toolchain:fedora-29-20190603

RUN sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
RUN sudo dnf -y install redhat-rpm-config python3-devel rsyslog cyrus-sasl iproute docker-ce-cli iptables-services openssl-devel libffi-devel

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

ADD docker/etc /etc
ADD requirements.txt requirements.txt

RUN pip3 install -U pip setuptools wheel
RUN pip3 install -r requirements.txt --ignore-installed

RUN ccm create cas-tmp --vnodes -n 1 --version=3.11.3
RUN cp -a ~/.ccm /.ccm
RUN pip3 uninstall -y ccm
