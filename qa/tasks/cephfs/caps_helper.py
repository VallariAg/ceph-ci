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
        dirname, filename = 'testdir', 'testfile'
        self.test_set = []

        # XXX: testpath is supposed contain a path inside CephFS (which might
        # be passed as an absolute path). Deleteing preceding '/' ensures that
        # previous path components aren't deleted by os.path.join.
        if testpath:
            testpath = testpath[1:] if testpath[0] == '/' else testpath
        # XXX: passing just '/' screw up os.path.join() ahead.
        if testpath == '/':
            testpath = ''

        for mount_x in mounts:
            dirpath = os_path_join(mount_x.hostfs_mntpt, testpath, dirname)
            mount_x.run_shell(f'mkdir {dirpath}')
            filepath = os_path_join(dirpath, filename)
            # XXX: the reason behind adding filepathm, cephfs_name and both
            # mntpts is to avoid a test bug where we mount cephfs1 but what
            # ends up being mounted cephfs2. since filepath and filedata are
            # identical, how would tests figure otherwise that they are
            # accessing the right filename but on wrong CephFS.
            filedata = (f'filepath = {filepath}\n'
                        f'cephfs_name = {mount_x.cephfs_name}\n'
                        f'cephfs_mntpt = {mount_x.cephfs_mntpt}\n'
                        f'hostfs_mntpt = {mount_x.hostfs_mntpt}')
            mount_x.write_file(filepath, filedata)
            self.test_set.append((mount_x, filepath, filedata))

    def run_cap_tests(self, perm, mntpt=None):
        # TODO
        #self.run_mon_cap_tests()
        self.run_mds_cap_tests(perm, mntpt=mntpt)

    def _get_fsnames_from_moncap(self, moncap):
        fsnames = []
        while moncap.find('fsname=') != -1:
            fsname_first_char = moncap.index('fsname=') + len('fsname=')

            if ',' in moncap:
                last = moncap.index(',')
                fsname = moncap[fsname_first_char : last]
                moncap = moncap.replace(moncap[0 : last+1], '')
            else:
                fsname = moncap[fsname_first_char : ]
                moncap = moncap.replace(moncap[0 : ], '')

            fsnames.append(fsname)

        return fsnames

    def run_mon_cap_tests(self, def_fs, moncap, client_id, keyring):
        """
        Check that MON cap is enforced for a client by searching for a Ceph
        FS name in output of cmd "fs ls" executed with that client's caps.

        XXX:
        We need access to admin_remote which can easily happen through any
        FS object and we need default FS to check case where no ceph fs name
        is mentioned in the MON cap. Getting default FS from the caller, thus,
        alone serves both the purpose.
        """
        get_cluster_cmd_op = def_fs.mon_manager.raw_cluster_cmd

        keyring_path = def_fs.admin_remote.mktemp(data=keyring)

        fsls = get_cluster_cmd_op(
            args=f'fs ls --id {client_id} -k {keyring_path}')

        # we need to check only for default FS when fsname clause is absent
        # in MON/MDS caps
        if 'fsname' not in moncap:
            self.assertIn(self.fs.name, fsls)
            return

        for fsname in self._get_fsnames_from_moncap(moncap):
            self.assertIn('name: ' + fsname, fsls)

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
            self.test_set = [(x, y.replace(mntpt, ''), z) for x, y, z in \
                             self.test_set]

        self.conduct_pos_test_for_read_caps()

        if perm == 'rw':
            self.conduct_pos_test_for_write_caps()
        elif perm == 'r':
            self.conduct_neg_test_for_write_caps()
        else:
            raise RuntimeError(f'perm = {perm}\nIt should be "r" or "rw".')

    def conduct_pos_test_for_read_caps(self):
        for mount, path, data in self.test_set:
            contents = mount.read_file(path)
            self.assertEqual(data, contents)

    def conduct_pos_test_for_write_caps(self):
        filedata = ('some new data on first fs', 'some new data on second fs')

        for mount, path, data in self.test_set:
            mount.write_file(path=path, data=data)
            contents = mount.read_file(path=path)
            self.assertEqual(data, contents)

    def conduct_neg_test_for_write_caps(self):
        possible_errmsgs = ('permission denied', 'operation not permitted')
        cmdargs = ['echo', 'some random data', Raw('|'), 'tee']

        # don't use data, cmd args to write are set already above.
        for mount, path, data in self.test_set:
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
