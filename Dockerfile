FROM docker.io/scylladb/scylla-toolchain:fedora-29-20190212

RUN sudo dnf -y install redhat-rpm-config python-devel

ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
