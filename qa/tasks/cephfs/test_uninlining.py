
"""
Test that data is uninlined using scrubbing.

The idea is to untar a linux-5.4.0 kernel tarball's Documentation/ dir
consisting of about 8000 files and uninline about 5145 of those which are
less than or equal to client_max_inline_size bytes and can be inlined when
written to while the inline_data config option is enabled.

This test runs across 2 active MDS, where a subset of the dirs under the
Documentation/ dir are pinned to either of the MDS.
"""

import logging

from io import StringIO
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from typing import List


log = logging.getLogger(__name__)


class InlineDataInfo:
    def __init__(self, length: int, version: int):
        self.inline_data_length = length
        self.inline_data_version = version


class TestDataUninlining(CephFSTestCase):
    MDSS_REQUIRED = 2

    # data version number of uninlined inode
    CEPH_INLINE_NONE = 18446744073709551615

    def remote_mntpt_cmd(self, cmd: str) -> str:
        final_cmd = f'cd {self.mount_a.hostfs_mntpt} && ' + cmd
        out = self.mount_a.client_remote.sh(final_cmd, stdout=StringIO())
        return out.strip()

    def extract_inodes(self, files: List[str]) -> List[str]:
        inodes = []
        for fil in files:
            cmd = f'ls -i {fil}'
            o = self.remote_mntpt_cmd(cmd)
            inodes.append(o.split(' ')[0])
        return inodes

    def get_inline_data_info(self, inodes: List[str]) -> List[InlineDataInfo]:
        info: List[InlineDataInfo] = []
        for inode in inodes:
            json_out = self.fs.rank_asok(['dump', 'inode', inode], 0)
            if json_out is None:
                json_out = self.fs.rank_asok(['dump', 'inode', inode], 1)
            self.assertTrue(json_out is not None)
            self.assertTrue('inline_data_length' in json_out)
            self.assertTrue('inline_data_version' in json_out)
            info.append(InlineDataInfo(json_out['inline_data_length'],
                                       json_out['inline_data_version']))
        return info

    def test_data_uninlining(self) -> None:
        # Set max_mds to 2
        self.fs.set_max_mds(2)

        # Enable inline_data
        self.fs.set_var('inline_data', '1', '--yes-i-really-really-mean-it')

        # Get configured max inline data size
        idsize = self.fs.fs_config.get('client_max_inline_size', 4096)
        idsize = int(idsize)

        # Fetch tarball
        cmd = 'wget http://download.ceph.com/qa/linux-5.4.tar.gz'
        self.remote_mntpt_cmd(cmd)

        # Extract test data tarball
        # FIXME
        # cmd = 'tar -x -z -f linux-5.4.tar.gz linux-5.4/Documentation/'
        cmd = 'tar -x -z -f linux-5.4.tar.gz'
        self.remote_mntpt_cmd(cmd)

        # Get dirs under linux-5.4.0/Documentation/
        # FIXME
        # cmd = 'find linux-5.4/ -mindepth 2 -maxdepth 2 -type d'
        cmd = 'find linux-5.4/ -mindepth 1 -maxdepth 1 -type d'
        o = self.remote_mntpt_cmd(cmd)
        dirs = o.split('\n')

        # Count files with size <= idsize
        cmd = f'find linux-5.4/ -type f -size -{idsize + 1}c'
        o = self.remote_mntpt_cmd(cmd)
        files = o.split('\n')

        # Dump file count
        log.info(f'Found {len(files)} inlined files')

        # Start recursive scrub on rank 0
        out_json = self.fs.run_scrub(["start", "/", "recursive"])

        # Pin dirs alternately to available mds
        for i in range(len(dirs)):
            self.mount_a.setfattr(dirs[i], 'ceph.dir.pin', str(i % 2))

        # Wait for scrub completion
        status = self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"],
                                                   timeout=43200)
        self.assertEqual(status, True)

        # Extract inode numbers of inlined files
        inodes = self.extract_inodes(files)

        # Dump inode info of files with size <= idsize
        self.assertEqual(len(files), len(inodes))

        info = self.get_inline_data_info(inodes)

        self.assertEqual(len(info), len(inodes))

        # Count files with inline_data_length == 0 and validate
        zero_length_count = 0
        for finfo in info:
            if int(finfo.inline_data_length) == 0:
                zero_length_count += 1
        log.info(f'Found {zero_length_count} files with '
                 'inline_data_length == 0')
        self.assertTrue(zero_length_count == len(files))

        # Count files with inline_data_version == 18446744073709551615
        # and validate
        uninlined_version_count = 0
        for finfo in info:
            if int(finfo.inline_data_version) == self.CEPH_INLINE_NONE:
                uninlined_version_count += 1
        log.info(f'Found {uninlined_version_count} files with '
                 'inline_data_version == CEPH_INLINE_NONE')
        self.assertTrue(uninlined_version_count == len(files))
