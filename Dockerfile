FROM scylladb/scylla-toolchain:fedora-29-20190603

RUN sudo dnf -y install redhat-rpm-config python3-devel

ADD requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
