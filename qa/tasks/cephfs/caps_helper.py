"""
Helper methods to test that MON and MDS caps are enforced properly.
"""
from os.path import join as os_path_join

from tasks.cephfs.cephfs_test_case import CephFSTestCase

from teuthology.orchestra.run import Raw


class CapTester(CephFSTestCase):
    """
    Test that MON and MDS caps are enforced.

    MDS caps are tested by exercising read-write permissions and MON caps are
    tested using output of command "ceph fs ls". Besides, it provides
    write_test_files() which creates test files at the given path on CephFS
    mounts passed to it.
    """

    def write_test_files(self, mounts, testpath=''):
        """
        Create file on mounts supplied at testpath. If testpath is None create
        file at CephFS's root.

        USAGE: Call this method at the beginning of the test and once the caps
        needs to be tested, call run_cap_tests(), run_mon_cap_tests() or
        run_mds_cap_tests() as per the need.
        """
        self.mounts = mounts
        self.filepaths, self.filedata = [], 'testdata'
        dirname, filename = 'testdir', 'testfile'

        # XXX: testpath is supposed contain a path inside CephFS (which might
        # be passed as an absolute path). Deleteing preceding '/' ensures that
        # previous path components aren't deleted by os.path.join.
        if testpath:
            testpath = testpath[1:] if testpath[0] == '/' else testpath
        # XXX: passing just '/' screw up os.path.join() ahead.
        if testpath == '/':
            testpath = ''

        for mount_x in self.mounts:
            dirpath = os_path_join(mount_x.hostfs_mntpt, testpath, dirname)
            mount_x.run_shell(f'mkdir {dirpath}')
            filepath = os_path_join(dirpath, filename)
            mount_x.write_file(filepath, self.filedata)
            self.filepaths.append(filepath)

    def run_cap_tests(self, perm, mntpt=None):
        # TODO
        #self.run_mon_cap_tests()
        self.run_mds_cap_tests(perm, mntpt=mntpt)

    def run_mon_cap_tests(self, moncap, keyring):
        """
        Check that MON cap is enforced for a client by searching for a Ceph
        FS name in output of cmd "fs ls" executed with that client's caps.
        """
        keyring_path = self.fs.admin_remote.mktemp(data=keyring)

        fsls = self.run_cluster_cmd(f'fs ls --id {self.client_id} -k '
                                    f'{keyring_path}')

        # we need to check only for default FS when fsname clause is absent
        # in MON/MDS caps
        if 'fsname' not in moncap:
            self.assertIn(self.fs.name, fsls)
            return

        fss = (self.fs1.name, self.fs2.name) if hasattr(self, 'fs1') else \
            (self.fs.name,)
        for fsname in fss:
            if fsname in moncap:
                self.assertIn('name: ' + fsname, fsls)
            else:
                self.assertNotIn('name: ' + fsname, fsls)

    def run_mds_cap_tests(self, perm, mntpt=None):
        """
        Run test for read perm and run write perm, run positive test if it
        is present and run negative test if it is not.
        """
        # XXX: mntpt is path inside cephfs that serves as root for current
        # mount. Therefore, this path must me deleted from self.filepaths.
        # Example -
        #   orignal path: /mnt/cephfs_x/dir1/dir2/testdir
        #   cephfs dir serving as root for current mnt: /dir1/dir2
        #   therefore, final path: /mnt/cephfs_x//testdir
        if mntpt:
            self.filepaths = [x.replace(mntpt, '') for x in self.filepaths]

        self.conduct_pos_test_for_read_caps()

        if perm == 'rw':
            self.conduct_pos_test_for_write_caps()
        elif perm == 'r':
            self.conduct_neg_test_for_write_caps()
        else:
            raise RuntimeError(f'perm = {perm}\nIt should be "r" or "rw".')

    def conduct_pos_test_for_read_caps(self):
        for mount in self.mounts:
            for path, data in zip(self.filepaths, (self.filedata,)):
                # XXX: conduct tests only if path belongs to current mount; in
                # teuth tests client are located on same machines.
                if path.find(mount.hostfs_mntpt) != -1:
                    contents = mount.read_file(path)
                    self.assertEqual(data, contents)

    def conduct_pos_test_for_write_caps(self):
        filedata = ('some new data on first fs', 'some new data on second fs')

        for mount in self.mounts:
            for path, data in zip(self.filepaths, (self.filedata,)):
                if path.find(mount.hostfs_mntpt) != -1:
                    # test that write was successful
                    mount.write_file(path=path, data=data)
                    # verify that contents written was same as the one that was
                    # intended
                    contents1 = mount.read_file(path=path)
                    self.assertEqual(data, contents1)

    def conduct_neg_test_for_write_caps(self):
        possible_errmsgs = ('permission denied', 'operation not permitted')
        cmdargs = ['echo', 'some random data', Raw('|'), 'tee']

        for mount in self.mounts:
            for path in self.filepaths:
                if path.find(mount.hostfs_mntpt) != -1:
                    cmdargs.append(path)
                    mount.negtestcmd(args=cmdargs, retval=1,
                                     errmsg=possible_errmsgs)
                    cmdargs.pop(len(cmdargs)-1)

    def get_mon_cap_from_keyring(self, client_name):
        keyring = self.run_cluster_cmd(cmd=f'auth get {client_name}')
        for line in keyring.split('\n'):
            if 'caps mon' in line:
                return line[line.find(' = "') + 4 : -1]

        raise RuntimeError('get_save_mon_cap: mon cap not found in keyring. '
                           'keyring -\n' + keyring)
