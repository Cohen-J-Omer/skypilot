"""Cloud object stores.

Currently, used for transferring data in bulk.  Thus, this module does not
offer file-level calls (e.g., open, reading, writing).

TODO:
* Better interface.
* Better implementation (e.g., fsspec, smart_open, using each cloud's SDK).
"""
import subprocess
import urllib.parse

from sky.clouds import gcp
from sky.data import data_utils
from sky.adaptors import aws


class CloudStorage:
    """Interface for a cloud object store."""

    def is_directory(self, url: str) -> bool:
        """Returns whether 'url' is a directory.

        In cloud object stores, a "directory" refers to a regular object whose
        name is a prefix of other objects.
        """
        raise NotImplementedError

    def make_sync_dir_command(self, source: str, destination: str) -> str:
        """Makes a runnable bash command to sync a 'directory'."""
        raise NotImplementedError

    def make_sync_file_command(self, source: str, destination: str) -> str:
        """Makes a runnable bash command to sync a file."""
        raise NotImplementedError


class S3CloudStorage(CloudStorage):
    """AWS Cloud Storage."""

    # List of commands to install AWS CLI
    _GET_AWSCLI = [
        'aws --version >/dev/null 2>&1 || pip3 install awscli',
    ]

    def is_directory(self, url: str) -> bool:
        """Returns whether S3 'url' is a directory.

        In cloud object stores, a "directory" refers to a regular object whose
        name is a prefix of other objects.
        """
        s3 = aws.resource('s3')
        bucket_name, path = data_utils.split_s3_path(url)
        bucket = s3.Bucket(bucket_name)

        num_objects = 0
        for obj in bucket.objects.filter(Prefix=path):
            num_objects += 1
            if obj.key == path:
                return False
            # If there are more than 1 object in filter, then it is a directory
            if num_objects == 3:
                return True

        # A directory with few or no items
        return True

    def make_sync_dir_command(self, source: str, destination: str) -> str:
        """Downloads using AWS CLI."""
        # AWS Sync by default uses 10 threads to upload files to the bucket.
        # To increase parallelism, modify max_concurrent_requests in your
        # aws config file (Default path: ~/.aws/config).
        download_via_awscli = ('aws s3 sync --no-follow-symlinks '
                               f'{source} {destination}')

        all_commands = list(self._GET_AWSCLI)
        all_commands.append(download_via_awscli)
        return ' && '.join(all_commands)

    def make_sync_file_command(self, source: str, destination: str) -> str:
        """Downloads a file using AWS CLI."""
        download_via_awscli = f'aws s3 cp {source} {destination}'

        all_commands = list(self._GET_AWSCLI)
        all_commands.append(download_via_awscli)
        return ' && '.join(all_commands)


class GcsCloudStorage(CloudStorage):
    """Google Cloud Storage."""

    # We use gsutil as a basic implementation.  One pro is that its -m
    # multi-threaded download is nice, which frees us from implementing
    # parellel workers on our end.
    # The gsutil command is part of the Google Cloud SDK, and we reuse
    # the installation logic here.
    _GET_GSUTIL = gcp.GCLOUD_INSTALLATION_COMMAND

    _GSUTIL = ('GOOGLE_APPLICATION_CREDENTIALS='
               f'{gcp.DEFAULT_GCP_APPLICATION_CREDENTIAL_PATH} gsutil')

    def is_directory(self, url: str) -> bool:
        """Returns whether 'url' is a directory.
        In cloud object stores, a "directory" refers to a regular object whose
        name is a prefix of other objects.
        """
        commands = [self._GET_GSUTIL]
        commands.append(f'{self._GSUTIL} ls -d {url}')
        command = ' && '.join(commands)
        p = subprocess.run(command,
                           stdout=subprocess.PIPE,
                           shell=True,
                           check=True,
                           executable='/bin/bash')
        out = p.stdout.decode().strip()
        # Edge Case: Gcloud command is run for first time #437
        out = out.split('\n')[-1]
        # If <url> is a bucket root, then we only need `gsutil` to succeed
        # to make sure the bucket exists. It is already a directory.
        _, key = data_utils.split_gcs_path(url)
        if len(key) == 0:
            return True
        # Otherwise, gsutil ls -d url will return:
        #   --> url.rstrip('/')          if url is not a directory
        #   --> url with an ending '/'   if url is a directory
        if not out.endswith('/'):
            assert out == url.rstrip('/'), (out, url)
            return False
        url = url if url.endswith('/') else (url + '/')
        assert out == url, (out, url)
        return True

    def make_sync_dir_command(self, source: str, destination: str) -> str:
        """Downloads a directory using gsutil."""
        download_via_gsutil = (
            f'{self._GSUTIL} -m rsync -r {source} {destination}')
        all_commands = [self._GET_GSUTIL]
        all_commands.append(download_via_gsutil)
        return ' && '.join(all_commands)

    def make_sync_file_command(self, source: str, destination: str) -> str:
        """Downloads a file using gsutil."""
        download_via_gsutil = f'{self._GSUTIL} -m cp {source} {destination}'
        all_commands = [self._GET_GSUTIL]
        all_commands.append(download_via_gsutil)
        return ' && '.join(all_commands)

class CosCloudStorage(CloudStorage):
    """IBM Cloud Storage."""
    _REGIONS = ['us-south', 'us-east', 'eu-de', 'eu-gb',
    'ca-tor', 'au-syd', 'br-sao', 'jp-osa', 'jp-tok']
    # install rclone if package isn't already installed
    _GET_RCLONE = [
        'rclone --version >/dev/null 2>&1 || curl https://rclone.org/install.sh | sudo bash',
    ]

    def _parse_bucket_from_url(self, url: str):
        """returns extracted bucket path from "cos://region/..." url"""
        if not url.startswith('cos://'):
            # received bucket name with possible path to bucket object
            return url
        parsed_url_values = url.split('//')[1].split('/',1)
        region = parsed_url_values[0]
        if region not in self._REGIONS:
            raise ValueError('region missing from IBM COS url.')
        return region, parsed_url_values[1] # returns bucket path

    def is_directory(self, url: str) -> bool:
        """Returns whether cos 'url' is a directory.

        In cloud object stores, a "directory" refers to a regular object whose
        name is a prefix of other objects.
        """

        region = url.split('//')[1].split('/')[0]
        s3 = data_utils.get_cos_resource(region)
        bucket_name, path = data_utils.split_cos_path(url, region)
        bucket = s3.Bucket(bucket_name)

        num_objects = 0
        for obj in bucket.objects.filter(Prefix=path):
            print(obj)

            num_objects += 1
            if obj.key == path:
                return False
            # If there are more than 1 object in filter, then it is a directory
            if num_objects == 3:
                return True

        # A directory with few or no items
        return True

    def get_cmd(self, source: str, destination: str, is_folder: bool):
        bucket_region, bucket_path = self._parse_bucket_from_url(source)
        rclone_config_data = data_utils.store_rclone_config(bucket_region)
        # store_rclone_config creates rclone config file at the cluster's nodes.
        store_rclone_config = (f'echo "{rclone_config_data}">> ~/.config/rclone/rclone.conf')
        mode = 'sync' if is_folder else 'copy'
        download_via_rclone = (f'rclone {mode} remote:{bucket_path} {destination}')

        all_commands = list(self._GET_RCLONE)
        all_commands.append(store_rclone_config)
        all_commands.append(download_via_rclone)
        return ' && '.join(all_commands)

    def make_sync_dir_command(self, source: str, destination: str) -> str:
        """Syncs a folder using rclone."""
        return self.get_cmd(source, destination, True)

    def make_sync_file_command(self, source: str, destination: str) -> str:
        """Copies a file using rclone."""
        return self.get_cmd(source, destination, False)

def get_storage_from_path(url: str) -> CloudStorage:
    """Returns a CloudStorage by identifying the scheme:// in a URL."""
    result = urllib.parse.urlsplit(url)

    if result.scheme not in _REGISTRY:
        assert False, (f'Scheme {result.scheme} not found in'
                       f' supported storage ({_REGISTRY.keys()}); path {url}')
    return _REGISTRY[result.scheme]


_REGISTRY = {
    'gs': GcsCloudStorage(),
    's3': S3CloudStorage(),
    'cos': CosCloudStorage()
}
