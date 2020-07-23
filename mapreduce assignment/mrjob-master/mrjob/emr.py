# -*- coding: utf-8 -*-
# Copyright 2009-2015 Yelp and Contributors
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
import hashlib
import json
import logging
import os
import os.path
import pipes
import random
import signal
import socket
import time
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from subprocess import Popen
from subprocess import PIPE

try:
    import boto
    import boto.ec2
    import boto.emr
    import boto.emr.connection
    import boto.emr.instance_group
    import boto.emr.emrobject
    import boto.exception
    import boto.https_connection
    import boto.regioninfo
    import boto.utils
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto = None

try:
    import filechunkio
except ImportError:
    # that's cool; filechunkio is only for multipart uploading
    filechunkio = None

import mrjob
import mrjob.step
from mrjob.aws import EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS
from mrjob.aws import EC2_INSTANCE_TYPE_TO_MEMORY
from mrjob.aws import MAX_STEPS_PER_JOB_FLOW
from mrjob.aws import emr_endpoint_for_region
from mrjob.aws import emr_ssl_host_for_region
from mrjob.aws import s3_location_constraint_for_region
from mrjob.compat import map_version
from mrjob.compat import version_gte
from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_lists
from mrjob.conf import combine_path_lists
from mrjob.conf import combine_paths
from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.s3 import S3Filesystem
from mrjob.fs.s3 import wrap_aws_conn
from mrjob.fs.ssh import SSHFilesystem
from mrjob.iam import FALLBACK_INSTANCE_PROFILE
from mrjob.iam import FALLBACK_SERVICE_ROLE
from mrjob.iam import get_or_create_mrjob_instance_profile
from mrjob.iam import get_or_create_mrjob_service_role
from mrjob.logs.ls import ls_logs
from mrjob.logparsers import best_error_from_logs
from mrjob.logparsers import scan_for_counters_in_files
from mrjob.parse import is_s3_uri
from mrjob.parse import is_uri
from mrjob.parse import iso8601_to_datetime
from mrjob.parse import iso8601_to_timestamp
from mrjob.parse import parse_s3_uri
from mrjob.parse import _parse_progress_from_job_tracker
from mrjob.parse import _parse_progress_from_resource_manager
from mrjob.patched_boto import patched_describe_cluster
from mrjob.patched_boto import patched_list_steps
from mrjob.pool import _est_time_to_hour
from mrjob.pool import _pool_hash_and_name
from mrjob.py2 import PY2
from mrjob.py2 import string_types
from mrjob.py2 import to_string
from mrjob.py2 import urlopen
from mrjob.retry import RetryGoRound
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.setup import BootstrapWorkingDirManager
from mrjob.setup import UploadDirManager
from mrjob.setup import parse_legacy_hash_path
from mrjob.setup import parse_setup_cmd
from mrjob.ssh import ssh_subordinate_addresses
from mrjob.ssh import ssh_terminate_single_job
from mrjob.util import cmd_line
from mrjob.util import shlex_split
from mrjob.util import random_identifier



log = logging.getLogger(__name__)

# how to set up the SSH tunnel for various AMI versions
_AMI_VERSION_TO_SSH_TUNNEL_CONFIG = {
    '2': dict(name='job tracker', path='/jobtracker.jsp', port=9100),
    '3': dict(name='resource manager', path='/cluster', port=9026),
    '4': dict(name='resource manager', path='/cluster', port=8088),
}

MAX_SSH_RETRIES = 20

# ssh should fail right away if it can't bind a port
WAIT_FOR_SSH_TO_FAIL = 1.0

# amount of time to wait between checks for available pooled job flows
JOB_FLOW_SLEEP_INTERVAL = 30.01  # Add .1 seconds so minutes arent spot on.

# bootstrap action which automatically terminates idle job flows
_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH = os.path.join(
    os.path.dirname(mrjob.__file__),
    'bootstrap',
    'terminate_idle_job_flow.sh')

# default AWS region to use for EMR. Using us-west-2 because it is the default
# for new (since October 10, 2012) accounts (see #1025)
_DEFAULT_AWS_REGION = 'us-west-2'

# default AMI to use on EMR. This will be updated with each version
_DEFAULT_AMI_VERSION = '3.7.0'

# Hadoop streaming jar on 1-3.x AMIs
_PRE_4_X_STREAMING_JAR = '/home/hadoop/contrib/streaming/hadoop-streaming.jar'

# intermediary jar used on 4.x AMIs
_4_X_INTERMEDIARY_JAR = 'command-runner.jar'


def s3_key_to_uri(s3_key):
    """Convert a boto Key object into an ``s3://`` URI"""
    return 's3://%s/%s' % (s3_key.bucket.name, s3_key.name)


def _repeat(api_call, *args, **kwargs):
    """Make the same API call repeatedly until we've seen every page
    of the response (sets *marker* automatically).

    Yields one or more responses.
    """
    marker = None

    while True:
        resp = api_call(*args, marker=marker, **kwargs)
        yield resp

        # go to next page, if any
        marker = getattr(resp, 'marker', None)
        if not marker:
            return


def _yield_all_clusters(emr_conn, *args, **kwargs):
    """Make successive API calls, yielding cluster summaries."""
    for resp in _repeat(emr_conn.list_clusters, *args, **kwargs):
        for cluster in getattr(resp, 'clusters', []):
            yield cluster


def _yield_all_bootstrap_actions(emr_conn, cluster_id, *args, **kwargs):
    for resp in _repeat(emr_conn.list_bootstrap_actions,
                        cluster_id, *args, **kwargs):
        for action in getattr(resp, 'actions', []):
            yield action


def _yield_all_instance_groups(emr_conn, cluster_id, *args, **kwargs):
    for resp in _repeat(emr_conn.list_instance_groups,
                        cluster_id, *args, **kwargs):
        for group in getattr(resp, 'instancegroups', []):
            yield group


def _yield_all_steps(emr_conn, cluster_id, *args, **kwargs):
    """Get all steps for the cluster, making successive API calls
    if necessary.

    Calls :py:func:`~mrjob.patched_boto.patched_list_steps`, to work around
    `boto's StartDateTime bug <https://github.com/boto/boto/issues/3268>`__.
    """
    for resp in _repeat(patched_list_steps, emr_conn, cluster_id,
                        *args, **kwargs):
        for step in getattr(resp, 'steps', []):
            yield step


def make_lock_uri(s3_tmp_dir, emr_job_flow_id, step_num):
    """Generate the URI to lock the job flow ``emr_job_flow_id``"""
    return s3_tmp_dir + 'locks/' + emr_job_flow_id + '/' + str(step_num)


def _lock_acquire_step_1(s3_fs, lock_uri, job_key, mins_to_expiration=None):
    bucket_name, key_prefix = parse_s3_uri(lock_uri)
    bucket = s3_fs.get_bucket(bucket_name)
    key = bucket.get_key(key_prefix)

    # EMRJobRunner should start using a job flow within about a second of
    # locking it, so if it's been a while, then it probably crashed and we
    # can just use this job flow.
    key_expired = False
    if key and mins_to_expiration is not None:
        last_modified = iso8601_to_datetime(key.last_modified)
        age = datetime.utcnow() - last_modified
        if age > timedelta(minutes=mins_to_expiration):
            key_expired = True

    if key is None or key_expired:
        key = bucket.new_key(key_prefix)
        key.set_contents_from_string(job_key.encode('utf_8'))
        return key
    else:
        return None


def _lock_acquire_step_2(key, job_key):
    key_value = key.get_contents_as_string()
    return (key_value == job_key.encode('utf_8'))


def attempt_to_acquire_lock(s3_fs, lock_uri, sync_wait_time, job_key,
                            mins_to_expiration=None):
    """Returns True if this session successfully took ownership of the lock
    specified by ``lock_uri``.
    """
    key = _lock_acquire_step_1(s3_fs, lock_uri, job_key, mins_to_expiration)
    if key is None:
        return False

    time.sleep(sync_wait_time)
    return _lock_acquire_step_2(key, job_key)


class EMRRunnerOptionStore(RunnerOptionStore):

    # documentation of these options is in docs/guides/emr-opts.rst

    ALLOWED_KEYS = RunnerOptionStore.ALLOWED_KEYS.union(set([
        'additional_emr_info',
        'ami_version',
        'aws_access_key_id',
        'aws_availability_zone',
        'aws_region',
        'aws_secret_access_key',
        'aws_security_token',
        'bootstrap',
        'bootstrap_actions',
        'bootstrap_cmds',
        'bootstrap_files',
        'bootstrap_python',
        'bootstrap_python_packages',
        'bootstrap_scripts',
        'check_emr_status_every',
        'ec2_core_instance_bid_price',
        'ec2_core_instance_type',
        'ec2_instance_type',
        'ec2_key_pair',
        'ec2_key_pair_file',
        'ec2_main_instance_bid_price',
        'ec2_main_instance_type',
        'ec2_subordinate_instance_type',
        'ec2_task_instance_bid_price',
        'ec2_task_instance_type',
        'emr_action_on_failure',
        'emr_api_params',
        'emr_endpoint',
        'emr_job_flow_id',
        'emr_job_flow_pool_name',
        'emr_tags',
        'enable_emr_debugging',
        'hadoop_streaming_jar_on_emr',
        'hadoop_version',
        'iam_instance_profile',
        'iam_endpoint',
        'iam_service_role',
        'max_hours_idle',
        'mins_to_end_of_hour',
        'num_ec2_core_instances',
        'num_ec2_instances',
        'num_ec2_task_instances',
        'pool_emr_job_flows',
        'pool_wait_minutes',
        'release_label',
        's3_endpoint',
        's3_log_uri',
        's3_sync_wait_time',
        's3_tmp_dir',
        's3_upload_part_size',
        'ssh_bin',
        'ssh_bind_ports',
        'ssh_tunnel',
        'ssh_tunnel_is_open',
        'visible_to_all_users',
    ]))

    COMBINERS = combine_dicts(RunnerOptionStore.COMBINERS, {
        'bootstrap': combine_lists,
        'bootstrap_actions': combine_lists,
        'bootstrap_cmds': combine_lists,
        'bootstrap_files': combine_path_lists,
        'bootstrap_python_packages': combine_path_lists,
        'bootstrap_scripts': combine_path_lists,
        'ec2_key_pair_file': combine_paths,
        'emr_api_params': combine_dicts,
        'emr_tags': combine_dicts,
        's3_log_uri': combine_paths,
        's3_tmp_dir': combine_paths,
        'ssh_bin': combine_cmds,
    })

    DEPRECATED_ALIASES = combine_dicts(RunnerOptionStore.DEPRECATED_ALIASES, {
        's3_scratch_uri': 's3_tmp_dir',
        'ssh_tunnel_to_job_tracker': 'ssh_tunnel',
    })

    def __init__(self, alias, opts, conf_paths):
        super(EMRRunnerOptionStore, self).__init__(alias, opts, conf_paths)

        # don't allow aws_region to be ''
        if not self['aws_region']:
            self['aws_region'] = _DEFAULT_AWS_REGION

        self._fix_ec2_instance_opts()
        self._fix_release_label_opt()

    def default_options(self):
        super_opts = super(EMRRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            'ami_version': _DEFAULT_AMI_VERSION,
            'aws_region': _DEFAULT_AWS_REGION,
            'bootstrap_python': True,
            'check_emr_status_every': 30,
            'cleanup_on_failure': ['JOB'],
            'ec2_core_instance_type': 'm1.medium',
            'ec2_main_instance_type': 'm1.medium',
            'emr_job_flow_pool_name': 'default',
            'mins_to_end_of_hour': 5.0,
            'num_ec2_core_instances': 0,
            'num_ec2_instances': 1,
            'num_ec2_task_instances': 0,
            'pool_wait_minutes': 0,
            's3_sync_wait_time': 5.0,
            's3_upload_part_size': 100,  # 100 MB
            'sh_bin': ['/bin/sh', '-ex'],
            'ssh_bin': ['ssh'],
            'ssh_bind_ports': list(range(40001, 40841)),
            'ssh_tunnel': False,
            'ssh_tunnel_is_open': False,
            'visible_to_all_users': True,
        })

    def _fix_ec2_instance_opts(self):
        """If the *ec2_instance_type* option is set, override instance
        type for the nodes that actually run tasks (see Issue #66). Allow
        command-line arguments to override defaults and arguments
        in mrjob.conf (see Issue #311).

        Also, make sure that core and subordinate instance type are the same,
        total number of instances matches number of main, core, and task
        instances, and that bid prices of zero are converted to None.
        """
        # Make sure subordinate and core instance type have the same value
        # Within EMRJobRunner we only ever use ec2_core_instance_type,
        # but we want ec2_subordinate_instance_type to be correct in the
        # options dictionary.
        if (self['ec2_subordinate_instance_type'] and
            (self._opt_priority['ec2_subordinate_instance_type'] >
             self._opt_priority['ec2_core_instance_type'])):
            self['ec2_core_instance_type'] = (
                self['ec2_subordinate_instance_type'])
        else:
            self['ec2_subordinate_instance_type'] = (
                self['ec2_core_instance_type'])

        # If task instance type is not set, use core instance type
        # (This is mostly so that we don't inadvertently join a pool
        # with task instance types with too little memory.)
        if not self['ec2_task_instance_type']:
            self['ec2_task_instance_type'] = (
                self['ec2_core_instance_type'])

        # Within EMRJobRunner, we use num_ec2_core_instances and
        # num_ec2_task_instances, not num_ec2_instances. (Number
        # of main instances is always 1.)
        if (self._opt_priority['num_ec2_instances'] >
            max(self._opt_priority['num_ec2_core_instances'],
                self._opt_priority['num_ec2_task_instances'])):
            # assume 1 main, n - 1 core, 0 task
            self['num_ec2_core_instances'] = (
                self['num_ec2_instances'] - 1)
            self['num_ec2_task_instances'] = 0
        else:
            # issue a warning if we used both kinds of instance number
            # options on the command line or in mrjob.conf
            if (self._opt_priority['num_ec2_instances'] >= 2 and
                self._opt_priority['num_ec2_instances'] <=
                max(self._opt_priority['num_ec2_core_instances'],
                    self._opt_priority['num_ec2_task_instances'])):
                log.warning('Mixing num_ec2_instances and'
                         ' num_ec2_{core,task}_instances does not make sense;'
                         ' ignoring num_ec2_instances')
            # recalculate number of EC2 instances
            self['num_ec2_instances'] = (
                1 +
                self['num_ec2_core_instances'] +
                self['num_ec2_task_instances'])

        # Allow ec2 instance type to override other instance types
        ec2_instance_type = self['ec2_instance_type']
        if ec2_instance_type:
            # core (subordinate) instances
            if (self._opt_priority['ec2_instance_type'] >
                max(self._opt_priority['ec2_core_instance_type'],
                    self._opt_priority['ec2_subordinate_instance_type'])):
                self['ec2_core_instance_type'] = ec2_instance_type
                self['ec2_subordinate_instance_type'] = ec2_instance_type

            # main instance only does work when it's the only instance
            if (self['num_ec2_core_instances'] <= 0 and
                self['num_ec2_task_instances'] <= 0 and
                (self._opt_priority['ec2_instance_type'] >
                 self._opt_priority['ec2_main_instance_type'])):
                self['ec2_main_instance_type'] = ec2_instance_type

            # task instances
            if (self._opt_priority['ec2_instance_type'] >
                    self._opt_priority['ec2_task_instance_type']):
                self['ec2_task_instance_type'] = ec2_instance_type

        # convert a bid price of '0' to None
        for role in ('core', 'main', 'task'):
            opt_name = 'ec2_%s_instance_bid_price' % role
            if not self[opt_name]:
                self[opt_name] = None
            else:
                # convert "0", "0.00" etc. to None
                try:
                    value = float(self[opt_name])
                    if value == 0:
                        self[opt_name] = None
                except ValueError:
                    pass  # maybe EMR will accept non-floats?

    def _fix_release_label_opt(self):
        """If *release_label* is not set and *ami_version* is set to version
        4 or higher (which the EMR API won't accept), set *release_label*
        to "emr-" plus *ami_version*. (Leave *ami_version* as-is;
        *release_label* overrides it anyway.)"""
        if (not self['release_label'] and
            self['ami_version'] != 'latest' and
            version_gte(self['ami_version'], '4')):
            self['release_label'] = 'emr-' + self['ami_version']


class EMRJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on Amazon Elastic MapReduce.
    Invoked when you run your job with ``-r emr``.

    :py:class:`EMRJobRunner` runs your job in an EMR job flow, which is
    basically a temporary Hadoop cluster. Normally, it creates a job flow
    just for your job; it's also possible to run your job in a specific
    job flow by setting *emr_job_flow_id* or to automatically choose a
    waiting job flow, creating one if none exists, by setting
    *pool_emr_job_flows*.

    Input, support, and jar files can be either local or on S3; use
    ``s3://...`` URLs to refer to files on S3.

    This class has some useful utilities for talking directly to S3 and EMR,
    so you may find it useful to instantiate it without a script::

        from mrjob.emr import EMRJobRunner

        emr_conn = EMRJobRunner().make_emr_conn()
        clusters = emr_conn.list_clusters()
        ...
    """
    alias = 'emr'

    # Don't need to bootstrap mrjob in the setup wrapper; that's what
    # the bootstrap script is for!
    BOOTSTRAP_MRJOB_IN_SETUP = False

    OPTION_STORE_CLASS = EMRRunnerOptionStore

    def __init__(self, **kwargs):
        """:py:class:`~mrjob.emr.EMRJobRunner` takes the same arguments as
        :py:class:`~mrjob.runner.MRJobRunner`, plus some additional options
        which can be defaulted in :ref:`mrjob.conf <mrjob.conf>`.

        *aws_access_key_id* and *aws_secret_access_key* are required if you
        haven't set them up already for boto (e.g. by setting the environment
        variables :envvar:`AWS_ACCESS_KEY_ID` and
        :envvar:`AWS_SECRET_ACCESS_KEY`)

        A lengthy list of additional options can be found in
        :doc:`guides/emr-opts.rst`.
        """
        super(EMRJobRunner, self).__init__(**kwargs)

        # if we're going to create a bucket to use as temp space, we don't
        # want to actually create it until we run the job (Issue #50).
        # This variable helps us create the bucket as needed
        self._s3_tmp_bucket_to_create = None

        self._fix_s3_tmp_and_log_uri_opts()

        # use job key to make a unique tmp dir
        self._s3_tmp_dir = self._opts['s3_tmp_dir'] + self._job_key + '/'

        # pick/validate output dir
        if self._output_dir:
            self._output_dir = self._check_and_fix_s3_dir(self._output_dir)
        else:
            self._output_dir = self._s3_tmp_dir + 'output/'

        # check AMI version
        if self._opts['ami_version'].startswith('1.'):
            log.warning('1.x AMIs will probably not work because they use'
                        ' Python 2.5. Use a later AMI version or mrjob v0.4.2')

        # manage working dir for bootstrap script
        self._bootstrap_dir_mgr = BootstrapWorkingDirManager()

        # manage local files that we want to upload to S3. We'll add them
        # to this manager just before we need them.
        s3_files_dir = self._s3_tmp_dir + 'files/'
        self._upload_mgr = UploadDirManager(s3_files_dir)

        # add the bootstrap files to a list of files to upload
        self._bootstrap_actions = []
        for action in self._opts['bootstrap_actions']:
            args = shlex_split(action)
            if not args:
                raise ValueError('bad bootstrap action: %r' % (action,))
            # don't use _add_bootstrap_file() because this is a raw bootstrap
            self._bootstrap_actions.append({
                'path': args[0],
                'args': args[1:],
            })

        if self._opts['bootstrap_files']:
            log.warning(
                "bootstrap_files is deprecated since v0.4.2 and will be"
                " removed in v0.6.0. Consider using bootstrap instead.")
        for path in self._opts['bootstrap_files']:
            self._bootstrap_dir_mgr.add(**parse_legacy_hash_path(
                'file', path, must_name='bootstrap_files'))

        self._bootstrap = self._bootstrap_python() + self._parse_bootstrap()
        self._legacy_bootstrap = self._parse_legacy_bootstrap()

        for cmd in self._bootstrap + self._legacy_bootstrap:
            for maybe_path_dict in cmd:
                if isinstance(maybe_path_dict, dict):
                    self._bootstrap_dir_mgr.add(**maybe_path_dict)

        if not (isinstance(self._opts['additional_emr_info'], string_types) or
                self._opts['additional_emr_info'] is None):
            self._opts['additional_emr_info'] = json.dumps(
                self._opts['additional_emr_info'])

        # where our own logs ended up (we'll find this out once we run the job)
        self._s3_job_log_uri = None

        # we'll create the script later
        self._main_bootstrap_script_path = None

        # the ID assigned by EMR to this job (might be None)
        self._cluster_id = self._opts['emr_job_flow_id']

        # when did our particular task start?
        self._emr_job_start = None

        # ssh state
        self._ssh_proc = None
        self._gave_cant_ssh_warning = False
        # we don't upload the ssh key to main until it's needed
        self._ssh_key_is_copied = False

        # cache for SSH address
        self._address = None
        self._ssh_subordinate_addrs = None

        # store the (tunneled) URL of the job tracker/resource manager
        self._tunnel_url = None

        # turn off tracker progress until tunnel is up
        self._show_tracker_progress = False

        # init hadoop, ami version caches
        self._ami_version = None
        self._hadoop_version = None

    def _fix_s3_tmp_and_log_uri_opts(self):
        """Fill in s3_tmp_dir and s3_log_uri (in self._opts) if they
        aren't already set.

        Helper for __init__.
        """
        # set s3_tmp_dir by checking for existing buckets
        if not self._opts['s3_tmp_dir']:
            self._set_s3_tmp_dir()
            log.info('using %s as our temp dir on S3' %
                     self._opts['s3_tmp_dir'])

        self._opts['s3_tmp_dir'] = self._check_and_fix_s3_dir(
            self._opts['s3_tmp_dir'])

        # set s3_log_uri
        if self._opts['s3_log_uri']:
            self._opts['s3_log_uri'] = self._check_and_fix_s3_dir(
                self._opts['s3_log_uri'])
        else:
            self._opts['s3_log_uri'] = self._opts['s3_tmp_dir'] + 'logs/'

    def _set_s3_tmp_dir(self):
        """Helper for _fix_s3_tmp_and_log_uri_opts"""
        buckets = self.fs.get_all_buckets()
        mrjob_buckets = [b for b in buckets if b.name.startswith('mrjob-')]

        # Loop over buckets until we find one that is not region-
        #   restricted, matches aws_region, or can be used to
        #   infer aws_region if no aws_region is specified
        for tmp_bucket in mrjob_buckets:
            tmp_bucket_name = tmp_bucket.name

            if (tmp_bucket.get_location() ==
                s3_location_constraint_for_region(self._opts['aws_region'])):

                # Regions are both specified and match
                log.info("using existing temp bucket %s" %
                         tmp_bucket_name)
                self._opts['s3_tmp_dir'] = ('s3://%s/tmp/' %
                                                tmp_bucket_name)
                return

        # That may have all failed. If so, pick a name.
        tmp_bucket_name = 'mrjob-' + random_identifier()
        self._s3_tmp_bucket_to_create = tmp_bucket_name
        log.info("creating new temp bucket %s" % tmp_bucket_name)
        self._opts['s3_tmp_dir'] = 's3://%s/tmp/' % tmp_bucket_name

    # TODO: stop fetching/accessing s3_job_log_uri directly
    def _log_dir(self):
        """Get the URI of the log directory for this job's cluster."""
        if not self._s3_job_log_uri:
            cluster = self._describe_cluster()
            self._set_s3_job_log_uri(cluster)

        return self._s3_job_log_uri

    def _set_s3_job_log_uri(self, cluster):
        """Given a job flow description, set self._s3_job_log_uri. This allows
        us to call self.fs.ls(), etc. without running the job.
        """
        log_uri = getattr(cluster, 'loguri', '')
        if log_uri:
            self._s3_job_log_uri = '%s%s/' % (
                log_uri.replace('s3n://', 's3://'), self._cluster_id)

    def _create_s3_tmp_bucket_if_needed(self):
        """Make sure temp bucket exists"""
        if self._s3_tmp_bucket_to_create:
            log.info('creating S3 bucket %r to use as temp space' %
                     self._s3_tmp_bucket_to_create)
            location = s3_location_constraint_for_region(
                self._opts['aws_region'])
            self.fs.create_bucket(
                self._s3_tmp_bucket_to_create, location=location)
            self._s3_tmp_bucket_to_create = None

    def _check_and_fix_s3_dir(self, s3_uri):
        """Helper for __init__"""
        if not is_s3_uri(s3_uri):
            raise ValueError('Invalid S3 URI: %r' % s3_uri)
        if not s3_uri.endswith('/'):
            s3_uri = s3_uri + '/'

        return s3_uri

    @property
    def fs(self):
        """:py:class:`~mrjob.fs.base.Filesystem` object for SSH, S3, and the
        local filesystem.
        """
        if self._fs is None:
            self._s3_fs = S3Filesystem(
                aws_access_key_id=self._opts['aws_access_key_id'],
                aws_secret_access_key=self._opts['aws_secret_access_key'],
                aws_security_token=self._opts['aws_security_token'],
                s3_endpoint=self._opts['s3_endpoint'])

            if self._opts['ec2_key_pair_file']:
                self._ssh_fs = SSHFilesystem(
                    ssh_bin=self._opts['ssh_bin'],
                    ec2_key_pair_file=self._opts['ec2_key_pair_file'])

                self._fs = CompositeFilesystem(self._ssh_fs, self._s3_fs,
                                               LocalFilesystem())
            else:
                self._ssh_fs = None
                self._fs = CompositeFilesystem(self._s3_fs, LocalFilesystem())

        return self._fs

    def _run(self):
        self._launch()
        self._wait_for_job_to_complete()

    def _launch(self):
        self._prepare_for_launch()
        self._launch_emr_job()

    def _prepare_for_launch(self):
        self._check_input_exists()
        self._check_output_not_exists()
        self._create_setup_wrapper_script()
        self._add_bootstrap_files_for_upload()
        self._add_job_files_for_upload()
        self._upload_local_files_to_s3()

    def _check_input_exists(self):
        """Make sure all input exists before continuing with our job.
        """
        if self._opts['check_input_paths']:
            for path in self._input_paths:
                if path == '-':
                    continue  # STDIN always exists

                if is_uri(path) and not is_s3_uri(path):
                    continue  # can't check non-S3 URIs, hope for the best

                if not self.fs.exists(path):
                    raise AssertionError(
                        'Input path %s does not exist!' % (path,))

    def _check_output_not_exists(self):
        """Verify the output path does not already exist. This avoids
        provisioning a cluster only to have Hadoop refuse to launch.
        """
        try:
            if self.fs.exists(self._output_dir):
                raise IOError(
                    'Output path %s already exists!' % (self._output_dir,))
        except boto.exception.S3ResponseError:
            pass

    def _add_bootstrap_files_for_upload(self, persistent=False):
        """Add files needed by the bootstrap script to self._upload_mgr.

        Tar up mrjob if bootstrap_mrjob is True.

        Create the main bootstrap script if necessary.

        persistent -- set by make_persistent_job_flow()
        """
        # lazily create mrjob.tar.gz
        if self._bootstrap_mrjob():
            self._create_mrjob_tar_gz()
            self._bootstrap_dir_mgr.add('file', self._mrjob_tar_gz_path)

        # all other files needed by the script are already in
        # _bootstrap_dir_mgr
        for path in self._bootstrap_dir_mgr.paths():
            self._upload_mgr.add(path)

        # now that we know where the above files live, we can create
        # the main bootstrap script
        self._create_main_bootstrap_script_if_needed()
        if self._main_bootstrap_script_path:
            self._upload_mgr.add(self._main_bootstrap_script_path)

        # make sure bootstrap action scripts are on S3
        for bootstrap_action in self._bootstrap_actions:
            self._upload_mgr.add(bootstrap_action['path'])

        # Add max-hours-idle script if we need it
        if (self._opts['max_hours_idle'] and
                (persistent or self._opts['pool_emr_job_flows'])):
            self._upload_mgr.add(_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH)

    def _add_job_files_for_upload(self):
        """Add files needed for running the job (setup and input)
        to self._upload_mgr."""
        for path in self._get_input_paths():
            self._upload_mgr.add(path)

        for path in self._working_dir_mgr.paths():
            self._upload_mgr.add(path)

        if self._opts['hadoop_streaming_jar']:
            self._upload_mgr.add(self._opts['hadoop_streaming_jar'])

        for step in self._get_steps():
            if step.get('jar'):
                self._upload_mgr.add(step['jar'])

    def _upload_local_files_to_s3(self):
        """Copy local files tracked by self._upload_mgr to S3."""
        self._create_s3_tmp_bucket_if_needed()

        log.info('Copying non-input files into %s' % self._upload_mgr.prefix)

        for path, s3_uri in self._upload_mgr.path_to_uri().items():
            log.debug('uploading %s -> %s' % (path, s3_uri))
            self._upload_contents(s3_uri, path)

    def _upload_contents(self, s3_uri, path):
        """Uploads the file at the given path to S3, possibly using
        multipart upload."""
        fsize = os.stat(path).st_size
        part_size = self._get_upload_part_size()

        s3_key = self.fs.make_s3_key(s3_uri)

        if self._should_use_multipart_upload(fsize, part_size, path):
            log.debug("Starting multipart upload of %s" % (path,))
            mpul = s3_key.bucket.initiate_multipart_upload(s3_key.name)

            try:
                self._upload_parts(mpul, path, fsize, part_size)
            except:
                mpul.cancel_upload()
                raise

            mpul.complete_upload()
            log.debug("Completed multipart upload of %s to %s" % (
                      path, s3_key.name))
        else:
            s3_key.set_contents_from_filename(path)

    def _upload_parts(self, mpul, path, fsize, part_size):
        offsets = range(0, fsize, part_size)

        for i, offset in enumerate(offsets):
            part_num = i + 1

            log.debug("uploading %d/%d of %s" % (
                part_num, len(offsets), path))
            chunk_bytes = min(part_size, fsize - offset)

            with filechunkio.FileChunkIO(
                    path, 'r', offset=offset, bytes=chunk_bytes) as fp:
                mpul.upload_part_from_file(fp, part_num)

    def _get_upload_part_size(self):
        # part size is in MB, as the minimum is 5 MB
        return int((self._opts['s3_upload_part_size'] or 0) * 1024 * 1024)

    def _should_use_multipart_upload(self, fsize, part_size, path):
        """Decide if we want to use multipart uploading.

        path is only used to log warnings."""
        if not part_size:  # disabled
            return False

        if fsize <= part_size:
            return False

        if filechunkio is None:
            log.warning("Can't use S3 multipart upload for %s because"
                        " filechunkio is not installed" % path)
            return False

        return True

    def _ssh_tunnel_config(self):
        """Look up AMI version, and return a dict with the following keys:

        name: "job tracker" or "resource manager"
        path: path to start page of job tracker/resource manager
        port: port job tracker/resource manager is running on.
        """
        return map_version(self.get_ami_version(),
                           _AMI_VERSION_TO_SSH_TUNNEL_CONFIG)

    def _set_up_ssh_tunnel(self, host):
        """set up the ssh tunnel to the job tracker, if it's not currently
        running.

        Args:
        host -- hostname of the EMR main node.
        """
        REQUIRED_OPTS = ['ec2_key_pair', 'ec2_key_pair_file', 'ssh_bind_ports']
        for opt_name in REQUIRED_OPTS:
            if not self._opts[opt_name]:
                if not self._gave_cant_ssh_warning:
                    log.warning(
                        "You must set %s in order to set up the SSH tunnel!" %
                        opt_name)
                    self._gave_cant_ssh_warning = True
                return

        # if there was already a tunnel, make sure it's still up
        if self._ssh_proc:
            self._ssh_proc.poll()
            if self._ssh_proc.returncode is None:
                return
            else:
                log.warning('Oops, ssh subprocess exited with return code %d,'
                            ' restarting...' % self._ssh_proc.returncode)
                self._ssh_proc = None

        # look up what we're supposed to do on this AMI version
        tunnel_config = self._ssh_tunnel_config()

        log.info('Opening ssh tunnel to %s' % tunnel_config['name'])

        # if ssh detects that a host key has changed, it will silently not
        # open the tunnel, so make a fake empty known_hosts file and use that.
        # (you can actually use /dev/null as your known hosts file, but
        # that's UNIX-specific)
        fake_known_hosts_file = os.path.join(
            self._get_local_tmp_dir(), 'fake_ssh_known_hosts')
        # blank out the file, if it exists
        f = open(fake_known_hosts_file, 'w')
        f.close()
        log.debug('Created empty ssh known-hosts file: %s' % (
            fake_known_hosts_file,))

        bind_port = None
        for bind_port in self._pick_ssh_bind_ports():
            args = self._opts['ssh_bin'] + [
                '-o', 'VerifyHostKeyDNS=no',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'ExitOnForwardFailure=yes',
                '-o', 'UserKnownHostsFile=%s' % fake_known_hosts_file,
                '-L', '%d:%s:%d' % (bind_port, host, tunnel_config['port']),
                '-N', '-q',  # no shell, no output
                '-i', self._opts['ec2_key_pair_file'],
            ]
            if self._opts['ssh_tunnel_is_open']:
                args.extend(['-g', '-4'])  # -4: listen on IPv4 only
            args.append('hadoop@' + host)
            log.debug('> %s' % cmd_line(args))

            ssh_proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            time.sleep(WAIT_FOR_SSH_TO_FAIL)
            ssh_proc.poll()
            # still running. We are golden
            if ssh_proc.returncode is None:
                self._ssh_proc = ssh_proc
                break
            else:
                ssh_proc.stdin.close()
                ssh_proc.stdout.close()
                print('ssh stderr: ' + to_string(ssh_proc.stderr.read()))
                ssh_proc.stderr.close()

        if not self._ssh_proc:
            log.warning(
                'Failed to open ssh tunnel to %s' % tunnel_config['name'])
        else:
            if self._opts['ssh_tunnel_is_open']:
                bind_host = socket.getfqdn()
            else:
                bind_host = 'localhost'
            self._tunnel_url = 'http://%s:%d%s' % (
                bind_host, bind_port, tunnel_config['path'])
            self._show_tracker_progress = True
            log.info('Connect to %s at: %s' % (
                tunnel_config['name'], self._tunnel_url))

    def _pick_ssh_bind_ports(self):
        """Pick a list of ports to try binding our SSH tunnel to.

        We will try to bind the same port for any given job flow (Issue #67)
        """
        # don't perturb the random number generator
        random_state = random.getstate()
        try:
            # seed random port selection on job flow ID
            random.seed(self._cluster_id)
            num_picks = min(MAX_SSH_RETRIES, len(self._opts['ssh_bind_ports']))
            return random.sample(self._opts['ssh_bind_ports'], num_picks)
        finally:
            random.setstate(random_state)

    ### Running the job ###

    def cleanup(self, mode=None):
        super(EMRJobRunner, self).cleanup(mode=mode)

        # always stop our SSH tunnel if it's still running
        if self._ssh_proc:
            self._ssh_proc.poll()
            if self._ssh_proc.returncode is None:
                log.info('Killing our SSH tunnel (pid %d)' %
                         self._ssh_proc.pid)

                self._ssh_proc.stdin.close()
                self._ssh_proc.stdout.close()
                self._ssh_proc.stderr.close()

                try:
                    os.kill(self._ssh_proc.pid, signal.SIGKILL)
                    self._ssh_proc = None
                except Exception as e:
                    log.exception(e)

        # stop the job flow if it belongs to us (it may have stopped on its
        # own already, but that's fine)
        # don't stop it if it was created due to --pool because the user
        # probably wants to use it again
        if self._cluster_id and not self._opts['emr_job_flow_id'] \
                and not self._opts['pool_emr_job_flows']:
            log.info('Terminating job flow: %s' % self._cluster_id)
            try:
                self.make_emr_conn().terminate_jobflow(self._cluster_id)
            except Exception as e:
                log.exception(e)

    def _cleanup_remote_tmp(self):
        # delete all the files we created
        if self._s3_tmp_dir:
            try:
                log.info('Removing all files in %s' % self._s3_tmp_dir)
                self.fs.rm(self._s3_tmp_dir)
                self._s3_tmp_dir = None
            except Exception as e:
                log.exception(e)

    def _cleanup_logs(self):
        super(EMRJobRunner, self)._cleanup_logs()

        # delete the log files, if it's a job flow we created (the logs
        # belong to the job flow)
        if self._s3_job_log_uri and not self._opts['emr_job_flow_id'] \
                and not self._opts['pool_emr_job_flows']:
            try:
                log.info('Removing all files in %s' % self._s3_job_log_uri)
                self.fs.rm(self._s3_job_log_uri)
                self._s3_job_log_uri = None
            except Exception as e:
                log.exception(e)

    def _cleanup_job(self):
        # kill the job if we won't be taking down the whole job flow
        if not (self._cluster_id or
                self._opts['emr_job_flow_id'] or
                self._opts['pool_emr_job_flows']):
            # we're taking down the job flow, don't bother
            return

        try:
            addr = self._address_of_main()
        except IOError:
            # if we can't get the address of the main node, job probably
            # isn't running
            return

        if not self._ran_job:
            if self._opts['ec2_key_pair_file']:
                try:
                    log.info("Attempting to terminate job...")
                    had_job = ssh_terminate_single_job(
                        self._opts['ssh_bin'],
                        addr,
                        self._opts['ec2_key_pair_file'])
                    if had_job:
                        log.info("Succeeded in terminating job")
                    else:
                        log.info("Job appears to have already been terminated")
                    return
                except IOError:
                    pass

            log.info('Unable to kill job without terminating job flow and'
                     ' job is still running. You may wish to terminate it'
                     ' yourself with "python -m mrjob.tools.emr.terminate_job_'
                     'flow %s".' % self._cluster_id)

    def _cleanup_job_flow(self):
        if not self._cluster_id:
            # If we don't have a job flow, then we can't terminate it.
            return

        emr_conn = self.make_emr_conn()
        try:
            log.info("Attempting to terminate job flow")
            emr_conn.terminate_jobflow(self._cluster_id)
        except Exception as e:
            # Something happened with boto and the user should know.
            log.exception(e)
            return
        log.info('Job flow %s successfully terminated' % self._cluster_id)

    def _wait_for_s3_eventual_consistency(self):
        """Sleep for a little while, to give S3 a chance to sync up.
        """
        log.info('Waiting %.1fs for S3 eventual consistency' %
                 self._opts['s3_sync_wait_time'])
        time.sleep(self._opts['s3_sync_wait_time'])

    def _cluster_is_done(self, cluster):
        return cluster.status.state in (
            'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS')

    def _wait_for_cluster_to_terminate(self):
        cluster = self._describe_cluster()

        if (cluster.status.state == 'WAITING' and
                cluster.autoterminate != 'true'):
            raise Exception('Operation requires job flow to terminate, but'
                            ' it may never do so.')

        while not self._cluster_is_done(cluster):
            msg = 'Waiting for job flow to terminate (currently %s)' % (
                cluster.status.state)
            log.info(msg)
            time.sleep(self._opts['check_emr_status_every'])
            cluster = self._describe_cluster()

    def _create_instance_group(self, role, instance_type, count, bid_price):
        """Helper method for creating instance groups. For use when
        creating a jobflow using a list of InstanceGroups, instead
        of the typical triumverate of
        num_instances/main_instance_type/subordinate_instance_type.

            - Role is either 'main', 'core', or 'task'.
            - instance_type is an EC2 instance type
            - count is an int
            - bid_price is a number, a string, or None. If None,
              this instance group will be use the ON-DEMAND market
              instead of the SPOT market.
        """

        if not instance_type:
            if self._opts['ec2_instance_type']:
                instance_type = self._opts['ec2_instance_type']
            else:
                raise ValueError('Missing instance type for %s node(s)' % role)

        if bid_price:
            market = 'SPOT'
            bid_price = str(bid_price)  # must be a string
        else:
            market = 'ON_DEMAND'
            bid_price = None

        # Just name the groups "main", "task", and "core"
        name = role.lower()

        return boto.emr.instance_group.InstanceGroup(
            count, role, instance_type, market, name, bidprice=bid_price)

    def _create_job_flow(self, persistent=False, steps=None):
        """Create an empty job flow on EMR, and return the ID of that
        job.

        If the ``emr_tags`` option is set, also tags the cluster (which
        is a separate API call).

        persistent -- if this is true, create the job flow with the keep_alive
            option, indicating the job will have to be manually terminated.
        """
        # make sure we can see the files we copied to S3
        self._wait_for_s3_eventual_consistency()

        log.info('Creating Elastic MapReduce job flow')
        args = self._job_flow_args(persistent, steps)

        emr_conn = self.make_emr_conn()
        log.debug('Calling run_jobflow(%r, %r, %s)' % (
            self._job_key, self._opts['s3_log_uri'],
            ', '.join('%s=%r' % (k, v) for k, v in args.items())))
        emr_job_flow_id = emr_conn.run_jobflow(
            self._job_key, self._opts['s3_log_uri'], **args)

         # keep track of when we started our job
        self._emr_job_start = time.time()

        log.info('Job flow created with ID: %s' % emr_job_flow_id)

        # set EMR tags for the cluster, if any
        tags = self._opts['emr_tags']
        if tags:
            log.info('Setting EMR tags: %s' % ', '.join(
                '%s=%s' % (tag, value or '') for tag, value in tags.items()))
            emr_conn.add_tags(emr_job_flow_id, tags)

        return emr_job_flow_id

    def _job_flow_args(self, persistent=False, steps=None):
        """Build kwargs for emr_conn.run_jobflow()"""
        args = {}
        api_params = {}

        if self._opts['release_label']:
            api_params['ReleaseLabel'] = self._opts['release_label']
        else:
            args['ami_version'] = self._opts['ami_version']

        if self._opts['aws_availability_zone']:
            args['availability_zone'] = self._opts['aws_availability_zone']
        # The old, simple API, available if we're not using task instances
        # or bid prices
        if not (self._opts['num_ec2_task_instances'] or
                self._opts['ec2_core_instance_bid_price'] or
                self._opts['ec2_main_instance_bid_price'] or
                self._opts['ec2_task_instance_bid_price']):
            args['num_instances'] = self._opts['num_ec2_core_instances'] + 1
            args['main_instance_type'] = (
                self._opts['ec2_main_instance_type'])
            args['subordinate_instance_type'] = self._opts['ec2_core_instance_type']
        else:
            # Create a list of InstanceGroups
            args['instance_groups'] = [
                self._create_instance_group(
                    'MASTER',
                    self._opts['ec2_main_instance_type'],
                    1,
                    self._opts['ec2_main_instance_bid_price']
                ),
            ]

            if self._opts['num_ec2_core_instances']:
                args['instance_groups'].append(
                    self._create_instance_group(
                        'CORE',
                        self._opts['ec2_core_instance_type'],
                        self._opts['num_ec2_core_instances'],
                        self._opts['ec2_core_instance_bid_price']
                    )
                )

            if self._opts['num_ec2_task_instances']:
                args['instance_groups'].append(
                    self._create_instance_group(
                        'TASK',
                        self._opts['ec2_task_instance_type'],
                        self._opts['num_ec2_task_instances'],
                        self._opts['ec2_task_instance_bid_price']
                    )
                )

        # bootstrap actions
        bootstrap_action_args = []

        for i, bootstrap_action in enumerate(self._bootstrap_actions):
            s3_uri = self._upload_mgr.uri(bootstrap_action['path'])
            bootstrap_action_args.append(
                boto.emr.BootstrapAction(
                    'action %d' % i, s3_uri, bootstrap_action['args']))

        if self._main_bootstrap_script_path:
            main_bootstrap_script_args = []
            if self._opts['pool_emr_job_flows']:
                main_bootstrap_script_args = [
                    'pool-' + self._pool_hash(),
                    self._opts['emr_job_flow_pool_name'],
                ]
            bootstrap_action_args.append(
                boto.emr.BootstrapAction(
                    'main',
                    self._upload_mgr.uri(self._main_bootstrap_script_path),
                    main_bootstrap_script_args))

        if persistent or self._opts['pool_emr_job_flows']:
            args['keep_alive'] = True

            # only use idle termination script on persistent job flows
            # add it last, so that we don't count bootstrapping as idle time
            if self._opts['max_hours_idle']:
                s3_uri = self._upload_mgr.uri(
                    _MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH)
                # script takes args in (integer) seconds
                ba_args = [int(self._opts['max_hours_idle'] * 3600),
                           int(self._opts['mins_to_end_of_hour'] * 60)]
                bootstrap_action_args.append(
                    boto.emr.BootstrapAction('idle timeout', s3_uri, ba_args))

        if bootstrap_action_args:
            args['bootstrap_actions'] = bootstrap_action_args

        if self._opts['ec2_key_pair']:
            args['ec2_keyname'] = self._opts['ec2_key_pair']

        if self._opts['enable_emr_debugging']:
            args['enable_debugging'] = True

        if self._opts['additional_emr_info']:
            args['additional_info'] = self._opts['additional_emr_info']

        # boto's connect_emr() has keyword args for these, but they override
        # emr_api_params, which is not what we want.
        api_params['VisibleToAllUsers'] = str(bool(
            self._opts['visible_to_all_users'])).lower()

        api_params['JobFlowRole'] = self._instance_profile()
        api_params['ServiceRole'] = self._service_role()

        if self._opts['emr_api_params']:
            api_params.update(self._opts['emr_api_params'])

        args['api_params'] = api_params

        if steps:
            args['steps'] = steps

        return args

    def _instance_profile(self):
        try:
            return (self._opts['iam_instance_profile'] or
                    get_or_create_mrjob_instance_profile(self.make_iam_conn()))
        except boto.exception.BotoServerError as ex:
            if ex.status != 403:
                raise
            log.warning(
                "Can't access IAM API, trying default instance profile: %s" %
                FALLBACK_INSTANCE_PROFILE)
            return FALLBACK_INSTANCE_PROFILE

    def _service_role(self):
        try:
            return (self._opts['iam_service_role'] or
                    get_or_create_mrjob_service_role(self.make_iam_conn()))
        except boto.exception.BotoServerError as ex:
            if ex.status != 403:
                raise
            log.warning(
                "Can't access IAM API, trying default service role: %s" %
                FALLBACK_SERVICE_ROLE)
            return FALLBACK_SERVICE_ROLE

    @property
    def _action_on_failure(self):
        # don't terminate other people's job flows
        if (self._opts['emr_action_on_failure']):
            return self._opts['emr_action_on_failure']
        elif (self._opts['emr_job_flow_id'] or
                self._opts['pool_emr_job_flows']):
            return 'CANCEL_AND_WAIT'
        else:
            return 'TERMINATE_CLUSTER'

    def _build_steps(self):
        """Return a list of boto Step objects corresponding to the
        steps we want to run."""
        # quick, add the other steps before the job spins up and
        # then shuts itself down (in practice this takes several minutes)
        return [self._build_step(n) for n in range(self._num_steps())]

    def _build_step(self, step_num):
        step = self._get_step(step_num)

        if step['type'] == 'streaming':
            return self._build_streaming_step(step_num)
        elif step['type'] == 'jar':
            return self._build_jar_step(step_num)
        else:
            raise AssertionError('Bad step type: %r' % (step['type'],))

    def _build_streaming_step(self, step_num):
        jar, step_arg_prefix = self._get_streaming_jar_and_step_arg_prefix()

        streaming_step_kwargs = dict(
            action_on_failure=self._action_on_failure,
            input=self._step_input_uris(step_num),
            jar=jar,
            name='%s: Step %d of %d' % (
                self._job_key, step_num + 1, self._num_steps()),
            output=self._step_output_uri(step_num),
        )

        step_args = []
        step_args.extend(step_arg_prefix)  # add 'hadoop-streaming' for 4.x
        step_args.extend(self._upload_args(self._upload_mgr))
        step_args.extend(self._hadoop_args_for_step(step_num))

        streaming_step_kwargs['step_args'] = step_args

        mapper, combiner, reducer = (
            self._hadoop_streaming_commands(step_num))

        streaming_step_kwargs['mapper'] = mapper
        streaming_step_kwargs['combiner'] = combiner
        streaming_step_kwargs['reducer'] = reducer

        return boto.emr.StreamingStep(**streaming_step_kwargs)

    def _build_jar_step(self, step_num):
        step = self._get_step(step_num)

        # special case to allow access to jars inside EMR
        if step['jar'].startswith('file:///'):
            jar = step['jar'][7:]  # keep leading slash
        else:
            jar = self._upload_mgr.uri(step['jar'])

        def interpolate(arg):
            if arg == mrjob.step.JarStep.INPUT:
                return ','.join(self._step_input_uris(step_num))
            elif arg == mrjob.step.JarStep.OUTPUT:
                return self._step_output_uri(step_num)
            else:
                return arg

        step_args = step['args']
        if step_args:
            step_args = [interpolate(arg) for arg in step_args]

        return boto.emr.JarStep(
            name='%s: Step %d of %d' % (
                self._job_key, step_num + 1, self._num_steps()),
            jar=jar,
            main_class=step['main_class'],
            step_args=step_args,
            action_on_failure=self._action_on_failure)

    def _get_streaming_jar_and_step_arg_prefix(self):
        if self._opts['hadoop_streaming_jar']:
            return self._upload_mgr.uri(self._opts['hadoop_streaming_jar']), []
        elif self._opts['hadoop_streaming_jar_on_emr']:
            return self._opts['hadoop_streaming_jar_on_emr'], []
        elif version_gte(self.get_ami_version(), '4'):
            # 4.x AMIs use an intermediary jar
            return _4_X_INTERMEDIARY_JAR, ['hadoop-streaming']
        else:
            # 2.x and 3.x AMIs just use a regular old streaming jar
            return _PRE_4_X_STREAMING_JAR, []

    def _launch_emr_job(self):
        """Create an empty jobflow on EMR, and set self._cluster_id to
        the ID for that job."""
        self._create_s3_tmp_bucket_if_needed()
        emr_conn = self.make_emr_conn()

        # try to find a job flow from the pool. basically auto-fill
        # 'emr_job_flow_id' if possible and then follow normal behavior.
        if self._opts['pool_emr_job_flows'] and not self._cluster_id:
            cluster_id = self._find_cluster(num_steps=len(self._get_steps()))
            if cluster_id:
                self._cluster_id = cluster_id

        # create a job flow if we're not already using an existing one
        if not self._cluster_id:
            self._cluster_id = self._create_job_flow(
                persistent=False)
            log.info('Created new job flow %s' %
                     self._cluster_id)
        else:
            log.info('Adding our job to existing job flow %s' %
                     self._cluster_id)

        # define out steps
        steps = self._build_steps()
        log.debug('Calling add_jobflow_steps(%r, %r)' % (
            self._cluster_id, steps))
        emr_conn.add_jobflow_steps(self._cluster_id, steps)

        # keep track of when we launched our job
        self._emr_job_start = time.time()

        # set EMR tags for the job, if any
        tags = self._opts['emr_tags']
        if tags:
            log.info('Setting EMR tags: %s' %
                    ', '.join('%s=%s' % (tag, value)
                              for tag, value in tags.items()))
            emr_conn.add_tags(self._cluster_id, tags)

    # TODO: break this method up; it's too big to write tests for
    def _wait_for_job_to_complete(self):
        """Wait for the job to complete, and raise an exception if
        the job failed.

        Also grab log URI from the job status (since we may not know it)
        """
        success = False

        while True:
            # don't antagonize EMR's throttling
            log.debug('Waiting %.1f seconds...' %
                      self._opts['check_emr_status_every'])
            time.sleep(self._opts['check_emr_status_every'])

            cluster = self._describe_cluster()

            self._set_s3_job_log_uri(cluster)

            job_state = cluster.status.state
            reason = getattr(
                getattr(cluster.status, 'statechangereason', None),
                'message', '')

            # find all steps belonging to us, and get their state
            step_states = []
            running_step_name = ''
            total_step_time = 0.0
            step_nums = []  # step numbers belonging to us. 1-indexed
            # "lg_step" stands for "log generating step"

            # TODO: this lg stuff confuses me and is about to be refactored
            # once I do some hand-testing on which steps actually generate
            # logs
            lg_step_num_mapping = {}

            steps = self._list_steps_for_cluster()
            latest_lg_step_num = 0

            def is_lg_step(step):
                return any(arg.value == '-mapper' for arg in step.config.args)

            for i, step in enumerate(steps):
                if is_lg_step(step):
                    latest_lg_step_num += 1

                # ignore steps belonging to other jobs
                if not step.name.startswith(self._job_key):
                    continue

                step_nums.append(i + 1)
                if is_lg_step(step):
                    lg_step_num_mapping[i + 1] = latest_lg_step_num

                step_states.append(step.status.state)
                if step.status.state == 'RUNNING':
                    running_step_name = step.name

                if (hasattr(step.status, 'timeline') and
                        hasattr(step.status.timeline, 'startdatetime') and
                        hasattr(step.status.timeline, 'enddatetime')):

                    start_time = iso8601_to_timestamp(
                        step.status.timeline.startdatetime)
                    end_time = iso8601_to_timestamp(
                        step.status.timeline.enddatetime)
                    total_step_time += end_time - start_time

            if not step_states:
                raise AssertionError("Can't find our steps in the job flow!")

            # if all our steps have completed, we're done!
            if all(state == 'COMPLETED' for state in step_states):
                success = True
                break

            # if any step fails, give up
            if any(state in ('CANCELLED', 'FAILED', 'INTERRUPTED')
                   for state in step_states):
                break

            # (the other step states are PENDING and RUNNING)

            # keep track of how long we've been waiting
            running_time = time.time() - self._emr_job_start

            # if a step is still running, we can print a status message
            if running_step_name:
                log.info('Job launched %.1fs ago, status %s: %s (%s)' %
                         (running_time, job_state, reason, running_step_name))

                if self._show_tracker_progress:
                    tunnel_config = self._ssh_tunnel_config()

                    tunnel_handle = None
                    try:
                        tunnel_handle = urlopen(self._tunnel_url)
                        tunnel_html = tunnel_handle.read()
                    except:
                        log.error('Unable to connect to %s' %
                                  tunnel_config['name'])
                        self._show_tracker_progress = False
                    else:
                        if tunnel_config['name'] == 'job tracker':
                            map_progress, reduce_progress = (
                                _parse_progress_from_job_tracker(tunnel_html))
                            if map_progress is not None:
                                log.info(' map %3d%% reduce %3d%%' % (
                                    map_progress, reduce_progress))
                        else:
                            progress = _parse_progress_from_resource_manager(
                                tunnel_html)
                            if progress is not None:
                                log.info(' %5.1f%% complete' % progress)
                    finally:
                        if tunnel_handle is not None:
                            tunnel_handle.close()

                # once a step is running, it's safe to set up the ssh tunnel to
                # the job tracker
                job_host = getattr(cluster, 'mainpublicdnsname', None)
                if job_host and self._opts['ssh_tunnel']:
                    self._set_up_ssh_tunnel(job_host)

            # other states include STARTING and SHUTTING_DOWN
            elif reason:
                log.info('Job launched %.1fs ago, status %s: %s' %
                         (running_time, job_state, reason))
            else:
                log.info('Job launched %.1fs ago, status %s' %
                         (running_time, job_state,))

        if success:
            log.info('Job completed.')
            log.info('Running time was %.1fs (not counting time spent waiting'
                     ' for the EC2 instances)' % total_step_time)
            self._fetch_counters(step_nums, lg_step_num_mapping)
            self.print_counters(range(1, len(step_nums) + 1))
        else:
            msg = 'Job on job flow %s failed with status %s: %s' % (
                cluster.id, job_state, reason)
            log.error(msg)
            # check for invalid fallback IAM roles
            if any(reason.rstrip().endswith('/%s is invalid' % role)
                   for role in (FALLBACK_INSTANCE_PROFILE,
                                FALLBACK_SERVICE_ROLE)):
                msg += (
                    '\n\n'
                    'Ask your admin to create the default EMR roles'
                    ' by following:\n\n'
                    '    http://docs.aws.amazon.com/ElasticMapReduce/latest'
                    '/DeveloperGuide/emr-iam-roles-creatingroles.html\n')
            else:
                if self._s3_job_log_uri:
                    log.info('Logs are in %s' % self._s3_job_log_uri)
                # look for a Python traceback
                cause = self._find_probable_cause_of_failure(
                    step_nums, sorted(lg_step_num_mapping.values()))
                if cause:
                    # log cause, and put it in exception
                    cause_msg = []  # lines to log and put in exception
                    cause_msg.append('Probable cause of failure (from %s):' %
                                     cause['log_file_uri'])
                    cause_msg.extend(
                        line.strip('\n') for line in cause['lines'])

                    if cause['input_uri']:
                        cause_msg.append('(while reading from %s)' %
                                         cause['input_uri'])

                    for line in cause_msg:
                        log.error(line)

                    # add cause_msg to exception message
                    msg += '\n' + '\n'.join(cause_msg) + '\n'

            raise Exception(msg)

    def _step_input_uris(self, step_num):
        """Get the s3:// URIs for input for the given step."""
        if step_num == 0:
            return [self._upload_mgr.uri(path)
                    for path in self._get_input_paths()]
        else:
            # put intermediate data in HDFS
            return ['hdfs:///tmp/mrjob/%s/step-output/%s/' % (
                self._job_key, step_num)]

    def _step_output_uri(self, step_num):
        if step_num == len(self._get_steps()) - 1:
            return self._output_dir
        else:
            # put intermediate data in HDFS
            return 'hdfs:///tmp/mrjob/%s/step-output/%s/' % (
                self._job_key, step_num + 1)

    ## LOG PARSING ##

    def _fetch_counters(self, step_nums, lg_step_num_mapping=None,
                        skip_s3_wait=False):
        """Read Hadoop counters from S3.

        Args:
        step_nums -- the steps belonging to us, so that we can ignore counters
                     from other jobs when sharing a job flow
        """
        # empty list is a valid value for lg_step_nums, but it is an optional
        # parameter
        if lg_step_num_mapping is None:
            lg_step_num_mapping = dict((n, n) for n in step_nums)
        lg_step_nums = sorted(
            lg_step_num_mapping[k] for k in step_nums
            if k in lg_step_num_mapping)

        self._counters = []
        new_counters = {}

        # TODO: do we need this?
        if not skip_s3_wait:
            self._wait_for_s3_eventual_consistency()

        uris = self._ls_logs('job', lg_step_nums)

        if uris:
            new_counters = scan_for_counters_in_files(
                uris, self.fs, self.get_hadoop_version())
        else:
            cluster = self._describe_cluster()
            if not self._cluster_is_done(cluster):
                log.info("Counters may not have been uploaded to S3 yet."
                         " Try again in 5 minutes with:"
                         " mrjob fetch-logs --counters %s" %
                         cluster.id)

        # step_nums is relative to the start of the job flow
        # we only want them relative to the job
        for step_num in step_nums:
            if step_num in lg_step_num_mapping:
                self._counters.append(
                    new_counters.get(lg_step_num_mapping[step_num], {}))
            else:
                self._counters.append({})

    def counters(self):
        return self._counters

    def _find_probable_cause_of_failure(self, step_nums, lg_step_nums=None):
        """Scan logs for Python exception tracebacks.

        :param step_nums: the numbers of steps belonging to us, so that we
                          can ignore errors from other jobs when sharing a job
                          flow
        :param lg_step_nums: "Log generating step numbers" - list of
                             (job flow step num, hadoop job num) mapping a job
                             flow step number to the number hadoop sees.
                             Necessary because not all steps generate task
                             attempt logs, and when there are steps that don't,
                             the number in the log path differs from the job
                             flow step number.

        Returns:
        None (nothing found) or a dictionary containing:
        lines -- lines in the log file containing the error message
        log_file_uri -- the log file containing the error message
        input_uri -- if the error happened in a mapper in the first
            step, the URI of the input file that caused the error
            (otherwise None)
        """
        if lg_step_nums is None:
            lg_step_nums = step_nums

        step_num_to_id = self._step_num_to_id()

        task_attempt_logs = self._ls_logs('task', step_nums,
                                          step_num_to_id=step_num_to_id)
        step_logs = self._ls_logs('step', step_nums,
                                  step_num_to_id=step_num_to_id)
        job_logs = self._ls_logs('job', step_nums,
                                 step_num_to_id=step_num_to_id)

        return best_error_from_logs(
            self.fs, task_attempt_logs, step_logs, job_logs)

    def _ls_logs(self, log_type, step_nums=None, step_num_to_id=None):
        # TODO: cache this as we go. We only need to know it if
        # step_nums is set
        if step_num_to_id is None:
            step_num_to_id = self._step_num_to_id()

        return ls_logs(self.fs, log_type,
                       log_dir=self._log_dir(),
                       ssh_host=self._address_of_main(),
                       step_nums=step_nums,
                       step_num_to_id=step_num_to_id)

    ### Bootstrapping ###

    def _create_main_bootstrap_script_if_needed(self):
        """Helper for :py:meth:`_add_bootstrap_files_for_upload`.

        Create the main bootstrap script and write it into our local
        temp directory. Set self._main_bootstrap_script_path.

        This will do nothing if there are no bootstrap scripts or commands,
        or if it has already been called."""
        if self._main_bootstrap_script_path:
            return

        # don't bother if we're not starting a job flow
        if self._opts['emr_job_flow_id']:
            return

        # Also don't bother if we're not bootstrapping
        if not (self._bootstrap or self._legacy_bootstrap or
                self._opts['bootstrap_files'] or
                self._bootstrap_mrjob()):
            return

        # create mrjob.tar.gz if we need it, and add commands to install it
        mrjob_bootstrap = []
        if self._bootstrap_mrjob():
            # _add_bootstrap_files_for_upload() should have done this
            assert self._mrjob_tar_gz_path
            path_dict = {
                'type': 'file', 'name': None, 'path': self._mrjob_tar_gz_path}
            self._bootstrap_dir_mgr.add(**path_dict)

            # find out where python keeps its libraries
            mrjob_bootstrap.append([
                "__mrjob_PYTHON_LIB=$(%s -c "
                "'from distutils.sysconfig import get_python_lib;"
                " print(get_python_lib())')" %
                cmd_line(self._python_bin())])
            # un-tar mrjob.tar.gz
            mrjob_bootstrap.append(
                ['sudo tar xfz ', path_dict, ' -C $__mrjob_PYTHON_LIB'])
            # re-compile pyc files now, since mappers/reducers can't
            # write to this directory. Don't fail if there is extra
            # un-compileable crud in the tarball (this would matter if
            # sh_bin were 'sh -e')
            mrjob_bootstrap.append(
                ['sudo %s -m compileall -f $__mrjob_PYTHON_LIB/mrjob && true' %
                 cmd_line(self._python_bin())])

        # we call the script b.py because there's a character limit on
        # bootstrap script names (or there was at one time, anyway)
        path = os.path.join(self._get_local_tmp_dir(), 'b.py')
        log.info('writing main bootstrap script to %s' % path)

        contents = self._main_bootstrap_script_content(
            self._bootstrap + mrjob_bootstrap + self._legacy_bootstrap)
        for line in contents:
            log.debug('BOOTSTRAP: ' + line.rstrip('\r\n'))

        with open(path, 'w') as f:
            for line in contents:
                f.write(line)

        self._main_bootstrap_script_path = path

    def _bootstrap_python(self):
        """Return a (possibly empty) list of parsed commands (in the same
        format as returned by parse_setup_cmd())'"""
        if not self._opts['bootstrap_python']:
            return []

        if PY2:
            # Python 2 is already installed; install pip and ujson

            # (We also install python-pip for bootstrap_python_packages,
            # but there's no harm in running these commands twice, and
            # bootstrap_python_packages is deprecated anyway.)
            return [
                ['sudo apt-get install -y python-pip || '
                 'sudo yum install -y python-pip'],
                ['sudo pip install --upgrade ujson'],
            ]
        else:
            # the best we can do is install the Python 3.4 package
            # (getting pip and ujson on Python 3 is much harder on EMR;
            # see docs/guides/emr-bootstrap-cookbook.rst)

            # we have to have at least on AMI 3.7.0
            if (self._opts['ami_version'] == 'latest' or
                not version_gte(self._opts['ami_version'], '3.7.0')):
                log.warning(
                    'bootstrapping Python 3 will probably not work on'
                    ' AMIs prior to 3.7.0. For an alternative, see:'
                    ' https://pythonhosted.org/mrjob/guides/emr-bootstrap'
                    '-cookbook.html#installing-python-from-source')

            return [['sudo yum install -y python34']]

        return []

    def _parse_bootstrap(self):
        """Parse the *bootstrap* option with
        :py:func:`mrjob.setup.parse_setup_cmd()`.
        """
        return [parse_setup_cmd(cmd) for cmd in self._opts['bootstrap']]

    def _parse_legacy_bootstrap(self):
        """Parse the deprecated
        options *bootstrap_python_packages*, and *bootstrap_cmds*
        *bootstrap_scripts* as bootstrap commands, in that order.

        This is a separate method from _parse_bootstrap() because bootstrapping
        mrjob happens after the new bootstrap commands (so you can upgrade
        Python) but before the legacy commands (for backwards compatibility).
        """
        bootstrap = []

        # bootstrap_python_packages
        if self._opts['bootstrap_python_packages']:
            if PY2:
                log.warning(
                    "bootstrap_python_packages is deprecated since v0.4.2 and"
                    " will be removed in v0.6.0. Consider using bootstrap"
                    " instead.")

                # this works on any AMI version
                bootstrap.append(['sudo apt-get install -y python-pip || '
                                  'sudo yum install -y python-pip'])

                for path in self._opts['bootstrap_python_packages']:
                    path_dict = parse_legacy_hash_path('file', path)
                    # don't worry about inspecting the tarball; pip is smart
                    # enough to deal with that
                    bootstrap.append(['sudo pip install ', path_dict])

            else:
                log.warning(
                    'bootstrap_python_packages is deprecated and is not'
                    ' supported on Python 3. See'
                    ' https://pythonhosted.org/mrjob/guides/emr-bootstrap'
                    '-cookbook.html#using-pip for an alternative.')

        # setup_cmds
        if self._opts['bootstrap_cmds']:
            log.warning(
                "bootstrap_cmds is deprecated since v0.4.2 and will be"
                " removed in v0.6.0. Consider using bootstrap instead.")
        for cmd in self._opts['bootstrap_cmds']:
            if not isinstance(cmd, string_types):
                cmd = cmd_line(cmd)
            bootstrap.append([cmd])

        # bootstrap_scripts
        if self._opts['bootstrap_scripts']:
            log.warning(
                "bootstrap_scripts is deprecated since v0.4.2 and will be"
                " removed in v0.6.0. Consider using bootstrap instead.")

        for path in self._opts['bootstrap_scripts']:
            path_dict = parse_legacy_hash_path('file', path)
            bootstrap.append([path_dict])

        return bootstrap

    def _main_bootstrap_script_content(self, bootstrap):
        """Create the contents of the main bootstrap script.
        """
        out = []

        def writeln(line=''):
            out.append(line + '\n')

        # shebang
        sh_bin = self._opts['sh_bin']
        if not sh_bin[0].startswith('/'):
            sh_bin = ['/usr/bin/env'] + sh_bin
        writeln('#!' + cmd_line(sh_bin))
        writeln()

        # store $PWD
        writeln('# store $PWD')
        writeln('__mrjob_PWD=$PWD')
        writeln()

        # download files
        writeln('# download files and mark them executable')

        if self._opts['release_label']:
            # on the 4.x AMIs, hadoop isn't yet installed, so use AWS CLI
            cp_to_local = 'aws s3 cp'
        else:
            # on the 2.x and 3.x AMIs, use hadoop
            cp_to_local = 'hadoop fs -copyToLocal'

        for name, path in sorted(
                self._bootstrap_dir_mgr.name_to_path('file').items()):
            uri = self._upload_mgr.uri(path)
            writeln('%s %s $__mrjob_PWD/%s' %
                    (cp_to_local, pipes.quote(uri), pipes.quote(name)))
            # make everything executable, like Hadoop Distributed Cache
            writeln('chmod a+x $__mrjob_PWD/%s' % pipes.quote(name))
        writeln()

        # run bootstrap commands
        writeln('# bootstrap commands')
        for cmd in bootstrap:
            # reconstruct the command line, substituting $__mrjob_PWD/<name>
            # for path dicts
            line = ''
            for token in cmd:
                if isinstance(token, dict):
                    # it's a path dictionary
                    line += '$__mrjob_PWD/'
                    line += pipes.quote(self._bootstrap_dir_mgr.name(**token))
                else:
                    # it's raw script
                    line += token
            writeln(line)
        writeln()

        return out

    ### EMR JOB MANAGEMENT UTILS ###

    def make_persistent_job_flow(self):
        """Create a new EMR job flow that requires manual termination, and
        return its ID.

        You can also fetch the job ID by calling self.get_emr_job_flow_id()
        """
        log.warning(
            'make_persistent_job_flow() has been renamed to'
            ' make_persistent_cluster(). This alias will be removed in v0.6.0')

        return self.make_persistent_cluster()

    def make_persistent_cluster(self):
        if (self._cluster_id):
            raise AssertionError(
                'This runner is already associated with job flow ID %s' %
                (self._cluster_id))

        log.info('Creating persistent job flow to run several jobs in...')

        self._add_bootstrap_files_for_upload(persistent=True)
        self._upload_local_files_to_s3()

        # don't allow user to call run()
        self._ran_job = True

        self._cluster_id = self._create_job_flow(persistent=True)

        return self._cluster_id

    def get_emr_job_flow_id(self):
        log.warning(
            'get_emr_job_flow_id() has been renamed to get_cluster_id().'
            ' This alias will be removed in v0.6.0')

        return self.get_cluster_id()

    # TODO: add to docs
    def get_cluster_id(self):
        return self._cluster_id

    def _usable_clusters(self, emr_conn=None, exclude=None, num_steps=1):
        """Get clusters that this runner can join.

        We basically expect to only join available clusters with the exact
        same setup as our own, that is:

        - same bootstrap setup (including mrjob version)
        - have the same AMI version
        - same number and type of instances

        However, we allow joining clusters where for each role, every instance
        has at least as much memory as we require, and the total number of
        compute units is at least what we require.

        There also must be room for our job in the cluster (clusters top out
        at 256 steps).

        We then sort by:
        - total compute units for core + task nodes
        - total compute units for main node
        - time left to an even instance hour

        The most desirable job flows come *last* in the list.

        :return: tuple of (:py:class:`botoemr.emrobject.Cluster`,
                           num_steps_in_cluster)
        """
        emr_conn = emr_conn or self.make_emr_conn()
        exclude = exclude or set()

        req_hash = self._pool_hash()

        # decide memory and total compute units requested for each
        # role type
        role_to_req_instance_type = {}
        role_to_req_num_instances = {}
        role_to_req_mem = {}
        role_to_req_cu = {}
        role_to_req_bid_price = {}

        for role in ('core', 'main', 'task'):
            instance_type = self._opts['ec2_%s_instance_type' % role]
            if role == 'main':
                num_instances = 1
            else:
                num_instances = self._opts['num_ec2_%s_instances' % role]

            role_to_req_instance_type[role] = instance_type
            role_to_req_num_instances[role] = num_instances

            role_to_req_bid_price[role] = (
                self._opts['ec2_%s_instance_bid_price' % role])

            # unknown instance types can only match themselves
            role_to_req_mem[role] = (
                EC2_INSTANCE_TYPE_TO_MEMORY.get(instance_type, float('Inf')))
            role_to_req_cu[role] = (
                num_instances *
                EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS.get(instance_type,
                                                       float('Inf')))

        # list of (sort_key, cluster_id, num_steps)
        key_cluster_steps_list = []

        def add_if_match(cluster):
            # this may be a retry due to locked job flows
            if cluster.id in exclude:
                return

            # only take persistent job flows
            if cluster.autoterminate != 'false':
                return

            # match pool name, and (bootstrap) hash
            bootstrap_actions = _yield_all_bootstrap_actions(
                emr_conn, cluster.id)
            pool_hash, pool_name = _pool_hash_and_name(bootstrap_actions)

            if req_hash != pool_hash:
                return

            if self._opts['emr_job_flow_pool_name'] != pool_name:
                return

            if self._opts['release_label']:
                # just check for exact match. EMR doesn't have a concept
                # of partial release labels like it does for AMI versions.
                if (getattr(cluster, 'releaselabel', '') !=
                    self._opts['release_label']):
                    return
            elif self._opts['ami_version'] == 'latest':
                # look for other clusters where "latest" was requested
                if getattr(cluster, 'requestedamiversion', '') != 'latest':
                    return
            else:
                # match actual AMI version
                ami_version = getattr(cluster, 'runningamiversion', '')
                # Support partial matches, e.g. let a request for
                # '2.4' pass if the version is '2.4.2'. The version
                # extracted from the existing job flow should always
                # be a full major.minor.patch, so checking matching
                # prefixes should be sufficient.
                if not ami_version.startswith(self._opts['ami_version']):
                    return

            steps = list(_yield_all_steps(emr_conn, cluster.id))

            # there is a hard limit of 256 steps per job flow
            if len(steps) + num_steps > MAX_STEPS_PER_JOB_FLOW:
                return

            # in rare cases, job flow can be WAITING *and* have incomplete
            # steps. We could just check for PENDING steps, but we're
            # trying to be defensive about EMR adding a new step state.
            for step in steps:
                if ((getattr(step.status, 'timeline', None) is None or
                     getattr(step.status.timeline, 'enddatetime', None)
                     is None) and
                    getattr(step.status, 'state', None) not in
                        ('CANCELLED', 'INTERRUPTED')):
                    return

            # total compute units per group
            role_to_cu = defaultdict(float)
            # total number of instances of the same type in each group.
            # This allows us to match unknown instance types.
            role_to_matched_instances = defaultdict(int)

            # check memory and compute units, bailing out if we hit
            # an instance with too little memory
            for ig in list(_yield_all_instance_groups(emr_conn, cluster.id)):
                role = ig.instancegrouptype.lower()

                # unknown, new kind of role; bail out!
                if role not in ('core', 'main', 'task'):
                    return

                req_instance_type = role_to_req_instance_type[role]
                if ig.instancetype != req_instance_type:
                    # if too little memory, bail out
                    mem = EC2_INSTANCE_TYPE_TO_MEMORY.get(ig.instancetype, 0.0)
                    req_mem = role_to_req_mem.get(role, 0.0)
                    if mem < req_mem:
                        return

                # if bid price is too low, don't count compute units
                req_bid_price = role_to_req_bid_price[role]
                bid_price = getattr(ig, 'bidprice', None)

                # if the instance is on-demand (no bid price) or bid prices
                # are the same, we're okay
                if bid_price and bid_price != req_bid_price:
                    # whoops, we didn't want spot instances at all
                    if not req_bid_price:
                        continue

                    try:
                        if float(req_bid_price) > float(bid_price):
                            continue
                    except ValueError:
                        # we don't know what to do with non-float bid prices,
                        # and we know it's not equal to what we requested
                        continue

                # don't require instances to be running; we'd be worse off if
                # we started our own job flow from scratch. (This can happen if
                # the previous job finished while some task instances were
                # still being provisioned.)
                cu = (int(ig.requestedinstancecount) *
                      EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS.get(
                          ig.instancetype, 0.0))
                role_to_cu.setdefault(role, 0.0)
                role_to_cu[role] += cu

                # track number of instances of the same type
                if ig.instancetype == req_instance_type:
                    role_to_matched_instances[role] += (
                        int(ig.requestedinstancecount))

            # check if there are enough compute units
            for role, req_cu in role_to_req_cu.items():
                req_num_instances = role_to_req_num_instances[role]
                # if we have at least as many units of the right type,
                # don't bother counting compute units
                if req_num_instances > role_to_matched_instances[role]:
                    cu = role_to_cu.get(role, 0.0)
                    if cu < req_cu:
                        return

            # make a sort key
            sort_key = (role_to_cu['core'] + role_to_cu['task'],
                        role_to_cu['main'],
                        _est_time_to_hour(cluster))

            key_cluster_steps_list.append((sort_key, cluster.id, len(steps)))

        for cluster_summary in _yield_all_clusters(
                emr_conn, cluster_states=['WAITING']):
            cluster = patched_describe_cluster(emr_conn, cluster_summary.id)
            add_if_match(cluster)

        return [(cluster_id, cluster_num_steps) for
                (sort_key, cluster_id, cluster_num_steps)
                in sorted(key_cluster_steps_list)]

    def _find_cluster(self, num_steps=1):
        """Find a job flow that can host this runner. Prefer flows with more
        compute units. Break ties by choosing flow with longest idle time.
        Return ``None`` if no suitable flows exist.
        """
        exclude = set()
        emr_conn = self.make_emr_conn()
        max_wait_time = self._opts['pool_wait_minutes']
        now = datetime.now()
        end_time = now + timedelta(minutes=max_wait_time)
        time_sleep = timedelta(seconds=JOB_FLOW_SLEEP_INTERVAL)

        log.info("Attempting to find an available job flow...")
        while now <= end_time:
            cluster_info_list = self._usable_clusters(
                emr_conn=emr_conn,
                exclude=exclude,
                num_steps=num_steps)
            if cluster_info_list:
                cluster_id, num_steps = cluster_info_list[-1]
                status = attempt_to_acquire_lock(
                    self.fs, self._lock_uri(cluster_id, num_steps),
                    self._opts['s3_sync_wait_time'], self._job_key)
                if status:
                    return cluster_id
                else:
                    exclude.add(cluster_id)
            elif max_wait_time == 0:
                return None
            else:
                # Reset the exclusion set since it is possible to reclaim a
                # lock that was previously unavailable.
                exclude = set()
                log.info("No job flows available in pool '%s'. Checking again"
                         " in %d seconds." % (
                             self._opts['emr_job_flow_pool_name'],
                             int(JOB_FLOW_SLEEP_INTERVAL)))
                time.sleep(JOB_FLOW_SLEEP_INTERVAL)
                now += time_sleep
        return None

    def _lock_uri(self, cluster_id, num_steps):
        return make_lock_uri(self._opts['s3_tmp_dir'],
                             cluster_id,
                             num_steps + 1)

    def _pool_hash(self):
        """Generate a hash of the bootstrap configuration so it can be used to
        match jobs and job flows. This first argument passed to the bootstrap
        script will be ``'pool-'`` plus this hash.

        The way the hash is calculated may vary between point releases
        (pooling requires the exact same version of :py:mod:`mrjob` anyway).
        """
        things_to_hash = [
            # exclude mrjob.tar.gz because it's only created if the
            # job starts its own job flow (also, its hash changes every time
            # since the tarball contains different timestamps).
            # The filenames/md5sums are sorted because we need to
            # ensure the order they're added doesn't affect the hash
            # here. Previously this used a dict, but Python doesn't
            # guarantee the ordering of dicts -- they can vary
            # depending on insertion/deletion order.
            sorted(
                (name, self.fs.md5sum(path)) for name, path
                in self._bootstrap_dir_mgr.name_to_path('file').items()
                if not path == self._mrjob_tar_gz_path),
            self._opts['additional_emr_info'],
            self._bootstrap,
            self._bootstrap_actions,
            self._opts['bootstrap_cmds'],
            self._bootstrap_mrjob(),
        ]

        if self._bootstrap_mrjob():
            things_to_hash.append(mrjob.__version__)

        things_json = json.dumps(things_to_hash, sort_keys=True)
        if not isinstance(things_json, bytes):
            things_json = things_json.encode('utf_8')

        m = hashlib.md5()
        m.update(things_json)
        return m.hexdigest()

    ### EMR-specific Stuff ###

    def make_emr_conn(self):
        """Create a connection to EMR.

        :return: a :py:class:`boto.emr.connection.EmrConnection`,
                 wrapped in a :py:class:`mrjob.retry.RetryWrapper`
        """
        # ...which is then wrapped in bacon! Mmmmm!

        # give a non-cryptic error message if boto isn't installed
        if boto is None:
            raise ImportError('You must install boto to connect to EMR')

        def emr_conn_for_endpoint(endpoint):
            conn = boto.emr.connection.EmrConnection(
                aws_access_key_id=self._opts['aws_access_key_id'],
                aws_secret_access_key=self._opts['aws_secret_access_key'],
                region=boto.regioninfo.RegionInfo(
                    name=self._opts['aws_region'], endpoint=endpoint,
                    connection_cls=boto.emr.connection.EmrConnection),
                security_token=self._opts['aws_security_token'])

            return conn

        endpoint = (self._opts['emr_endpoint'] or
                    emr_endpoint_for_region(self._opts['aws_region']))

        log.debug('creating EMR connection (to %s)' % endpoint)
        conn = emr_conn_for_endpoint(endpoint)

        # Issue #621: if we're using a region-specific endpoint,
        # try both the canonical version of the hostname and the one
        # that matches the SSL cert
        if not self._opts['emr_endpoint']:

            ssl_host = emr_ssl_host_for_region(self._opts['aws_region'])
            fallback_conn = emr_conn_for_endpoint(ssl_host)

            conn = RetryGoRound(
                [conn, fallback_conn],
                lambda ex: isinstance(
                    ex, boto.https_connection.InvalidCertificateException))

        return wrap_aws_conn(conn)

    def _describe_cluster(self):
        emr_conn = self.make_emr_conn()
        return patched_describe_cluster(emr_conn, self._cluster_id)

    def _list_steps_for_cluster(self):
        """Get all steps for our cluster, potentially making multiple API calls
        """
        emr_conn = self.make_emr_conn()
        return list(_yield_all_steps(emr_conn, self._cluster_id))

    def _step_num_to_id(self):
        if self._cluster_id is None:
            return {}

        return dict((step_num, step.id)
            for step_num, step in
            enumerate(self._list_steps_for_cluster(), start=1))

    def get_hadoop_version(self):
        if self._hadoop_version is None:
            self._store_cluster_info()
        return self._hadoop_version

    def get_ami_version(self):
        """Get the AMI that our job flow is running.

        .. versionadded:: 0.4.5
        """
        if self._ami_version is None:
            self._store_cluster_info()
        return self._ami_version

    def _store_cluster_info(self):
        """Set self._ami_version and self._hadoop_version."""
        if not self._cluster_id:
            raise AssertionError('cluster has not yet been created')

        cluster = self._describe_cluster()

        # AMI version might be in RunningAMIVersion (2.x, 3.x)
        # or ReleaseLabel (4.x)
        self._ami_version = getattr(cluster, 'runningamiversion', None)
        if not self._ami_version:
            release_label = getattr(cluster, 'releaselabel', None)
            if release_label:
                self._ami_version = release_label.lstrip('emr-')

        for a in cluster.applications:
            if a.name.lower() == 'hadoop':  # 'Hadoop' on 4.x AMIs
                self._hadoop_version = a.version

        # TODO: could get mainpublicdnsname too

    def _address_of_main(self, emr_conn=None):
        """Get the address of the main node so we can SSH to it"""
        # cache address of main to avoid redundant calls to describe_jobflow
        # also convenient for testing (pretend we can SSH when we really can't
        # by setting this to something not False)
        if not self._address:
            try:
                cluster = self._describe_cluster()
                if cluster.status.state not in ('RUNNING', 'WAITING'):
                    return None
                self._address = cluster.mainpublicdnsname
            except boto.exception.S3ResponseError:
                # Raised when cluster doesn't exist
                pass

        return self._address

    def _addresses_of_subordinates(self):
        if not self._ssh_subordinate_addrs:
            self._ssh_subordinate_addrs = ssh_subordinate_addresses(
                self._opts['ssh_bin'],
                self._address_of_main(),
                self._opts['ec2_key_pair_file'])
        return self._ssh_subordinate_addrs

    def make_iam_conn(self):
        """Create a connection to S3.

        :return: a :py:class:`boto.iam.connection.IAMConnection`, wrapped in a
                 :py:class:`mrjob.retry.RetryWrapper`
        """
        # give a non-cryptic error message if boto isn't installed
        if boto is None:
            raise ImportError('You must install boto to connect to IAM')

        host = self._opts['iam_endpoint'] or 'iam.amazonaws.com'

        log.debug('creating IAM connection to %s' % host)

        raw_iam_conn = boto.connect_iam(
            aws_access_key_id=self._opts['aws_access_key_id'],
            aws_secret_access_key=self._opts['aws_secret_access_key'],
            host=host,
            security_token=self._opts['aws_security_token'])

        return wrap_aws_conn(raw_iam_conn)
