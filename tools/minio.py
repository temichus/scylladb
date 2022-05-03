import os

from ccmlib.common import check_socket_listening
import docker


def running_in_docker():
    path = '/proc/self/cgroup'
    with open(path) as cgroup:
        return (
            os.path.exists('/.dockerenv') or
            os.path.isfile(path) and any('docker' in line for line in cgroup)
        )


class MinioDocker:
    def __init__(self, name, image='minio/minio:latest'):
        self.name = name
        self.container = None
        self.port = None
        self.address = None
        self.image = image
        self.access_key = "test1"
        self.secret_key = "12345678"

    def __enter__(self):
        self.create_minio_container()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove_container()

    def create_minio_container(self):
        if self.container:
            raise Exception('minio docker already exists for this instance')
        docker_client = docker.from_env()
        docker_client.images.pull(self.image)
        docker_client.containers.run(ports={'9000/tcp': None},
                                     name=self.name,
                                     environment=[f'MINIO_ACCESS_KEY={self.access_key}',
                                                  f'MINIO_SECRET_KEY={self.secret_key}'],
                                     image=self.image,
                                     detach=True,
                                     command="server /data",
                                     labels=['dtest'], remove=True)

        for container in docker_client.containers.list():
            if self.name in container.name:
                self.container = container
                if running_in_docker():
                    self.port = '9000'
                    self.address = container.attrs['NetworkSettings']['IPAddress']
                else:
                    self.port = container.ports['9000/tcp'][0]['HostPort']
                    self.address = 'localhost'

        if self.container:
            check_socket_listening((self.address, int(self.port)), timeout=20)

    def remove_container(self):
        self.container.remove(force=True)
        self.container = None

    @property
    def endpoint_url(self):
        return f'http://{self.address}:{self.port}'


if __name__ == "__main__":
    import boto3
    import uuid
    with MinioDocker(name=f"test{str(uuid.uuid4())[:8]}") as minio:

        client = boto3.client(service_name='s3',
                              aws_access_key_id=minio.access_key,
                              aws_secret_access_key=minio.secret_key,
                              endpoint_url=minio.endpoint_url)

        client.create_bucket(Bucket="test1")
