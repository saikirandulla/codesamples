# Copyright 2009-2012 Yelp and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import posixpath

from io import BytesIO
from mrjob.fs.base import Filesystem
from mrjob.ssh import ssh_cat
from mrjob.ssh import ssh_copy_key
from mrjob.ssh import ssh_ls
from mrjob.ssh import ssh_subordinate_addresses
from mrjob.ssh import SSH_PREFIX
from mrjob.ssh import SSH_URI_RE
from mrjob.util import random_identifier
from mrjob.util import read_file


log = logging.getLogger(__name__)


class SSHFilesystem(Filesystem):
    """Filesystem for remote systems accessed via SSH. Typically you will get
    one of these via ``EMRJobRunner().fs``, composed with
    :py:class:`~mrjob.fs.s3.S3Filesystem` and
    :py:class:`~mrjob.fs.local.LocalFilesystem`.
    """

    def __init__(self, ssh_bin, ec2_key_pair_file):
        """
        :param ssh_bin: path to ``ssh`` binary
        :param ec2_key_pair_file: path to an SSH keyfile
        """
        super(SSHFilesystem, self).__init__()
        self._ssh_bin = ssh_bin
        self._ec2_key_pair_file = ec2_key_pair_file
        if self._ec2_key_pair_file is None:
            raise ValueError('ec2_key_pair_file must be a path')

        # keep track of which hosts we've copied our key to, and
        # what the (random) name of the key file is on that host
        self._host_to_key_filename = {}

        # keep track of the subordinate hosts accessible through each host
        self._host_to_subordinate_hosts = {}

    def can_handle_path(self, path):
        return SSH_URI_RE.match(path) is not None

    def du(self, path_glob):
        raise IOError()  # not implemented

    def ls(self, path_glob):
        if SSH_URI_RE.match(path_glob):
            for item in self._ssh_ls(path_glob):
                yield item
            return

    def _key_filename_for(self, addr):
        """If *addr* is a !-separated pair of hosts like ``main!subordinate``,
        get the name of the copy of our keypair file on ``main``. If there
        isn't one, pick a random name, and copy the key file there.

        Otherwise, return ``None``."""
        # don't need to copy a key if we're SSHing directly
        if '!' not in addr:
            return None

        host = addr.split('!')[0]

        if host not in self._host_to_key_filename:
            # copy the key if we haven't already
            keyfile = 'mrjob-%s.pem' % random_identifier()
            ssh_copy_key(self._ssh_bin, host, self._ec2_key_pair_file, keyfile)
            # don't set above; ssh_copy_key() may throw an IOError
            self._host_to_key_filename[host] = keyfile

        return self._host_to_key_filename[host]

    def _ssh_ls(self, uri):
        """Helper for ls(); obeys globbing"""
        m = SSH_URI_RE.match(uri)
        addr = m.group('hostname')
        if not addr:
            raise ValueError

        keyfile = self._key_filename_for(addr)

        output = ssh_ls(
            self._ssh_bin,
            addr,
            self._ec2_key_pair_file,
            m.group('filesystem_path'),
            keyfile,
        )

        for line in output:
            # skip directories, we only want to return downloadable files
            if line and not line.endswith('/'):
                yield SSH_PREFIX + addr + line

    def md5sum(self, path):
        raise IOError()  # not implemented

    def _cat_file(self, filename):
        ssh_match = SSH_URI_RE.match(filename)
        addr = ssh_match.group('hostname') or self._address_of_main()

        keyfile = self._key_filename_for(addr)

        output = ssh_cat(
            self._ssh_bin,
            addr,
            self._ec2_key_pair_file,
            ssh_match.group('filesystem_path'),
            keyfile,
        )
        return read_file(filename, fileobj=BytesIO(output))

    def mkdir(self, dest):
        raise IOError()  # not implemented

    def exists(self, path_glob):
        # just fall back on ls(); it's smart
        try:
            return any(self.ls(path_glob))
        except IOError:
            return False

    def rm(self, path_glob):
        raise IOError()  # not implemented

    def touchz(self, dest):
        raise IOError()  # not implemented

    def ssh_subordinate_hosts(self, host, force=False):
        """Get a list of the subordinate hosts reachable through *hosts*"""
        if force or host not in self._host_to_subordinate_hosts:
            self._host_to_subordinate_hosts[host] = ssh_subordinate_addresses(
                self._ssh_bin, host, self._ec2_key_pair_file)

        return self._host_to_subordinate_hosts[host]
