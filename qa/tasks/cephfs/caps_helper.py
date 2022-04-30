"""
Helper methods to test that MON and MDS caps are enforced properly.
"""
from os.path import join as os_path_join
from logging import getLogger

from tasks.cephfs.cephfs_test_case import CephFSTestCase

from teuthology.orchestra.run import Raw


log = getLogger(__name__)


class CapsUtil():

    def gen_moncap_str(self, caps):
        """
        Expects a tuple of tuples where the inner tuple has perm and CephFS
        name.
        """
        def _gen_moncap_str(c):
            perm, fsname = c
            moncap = f'allow {perm}'
            if fsname:
                moncap += f' fsname={fsname}'
            return moncap

        if len(caps) == 1:
            return _gen_moncap_str(caps[0])

        moncap = ''
        for i, c in enumerate(caps):
            moncap += _gen_moncap_str(c)
            if i != len(caps) - 1:
                moncap += ', '

        return moncap

    def gen_osdcap_str(self, caps):
        """
        Expects a tuple of tuples where the inner tuple has perm and data
        pool names.
        """
        def _gen_osdcap_str(c):
            perm, datapoolname = c
            osdcap = f'allow {perm} tag cephfs'
            if datapoolname:
                osdcap += f' data={datapoolname}'
            return osdcap

        if len(caps) == 1:
            return _gen_osdcap_str(caps[0])

        osdcap = ''
        for i, c in enumerate(caps):
            osdcap += _gen_osdcap_str(c)
            if i != len(caps) - 1:
                osdcap += ', '

        return osdcap

    def gen_mdscap_str(self, caps):
        """
        Expects a tuple of tuples where inner tuple has perm an Ceph FS name
        and CephFS mountpoint.
        """
        def _unpack_tuple(c):
            try:
                perm, fsname, cephfs_mntpt = c
            except ValueError:
                perm, fsname = c
                cephfs_mntpt = '/'
            return perm, fsname, cephfs_mntpt

        def _gen_mdscap_str(c):
            perm, fsname, cephfs_mntpt = _unpack_tuple(c)
            mdscap = f'allow {perm}'
            if fsname:
                mdscap += f' fsname={fsname}'
            if cephfs_mntpt != '/':
                mdscap += f' path={cephfs_mntpt}'
            return mdscap

            return _gen_mdscap_str(caps[0])

        mdscap = ''
        for i, c in enumerate(caps):
            mdscap +=  _gen_mdscap_str(c)
            if i != len(caps) - 1:
                mdscap += ', '

        return mdscap


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
            log.info(f'creating test file on FS {mount_x.cephfs_name} '
                     f'mounted at {mount_x.mountpoint}...')
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
            log.info('test file created at {path} with data "{data}.')

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
        log.info(f'output of fs ls cmd run by client.{client_id} -\n{fsls}')

        if 'fsname=' not in moncap:
            log.info('no FS name is mentioned in moncap, client has '
                     'permission to list all files. moncap -\n{moncap}')
            log.info('testing for presence of all FS names in output of '
                     '"fs ls" command run by client.')

            fsls_admin = get_cluster_cmd_op(args=f'fs ls')
            log.info('output of fs ls cmd run by admin -\n{fsls_admin}')

            self.assertEqual(fsls, fsls_admin)
            return

        log.info('FS names are mentioned in moncap. moncap -\n{moncap}')
        log.info('testing for presence of these FS names in output of '
                 '"fs ls" command run by client.')
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
            log.info(f'test read perm: read file {path} and expect data '
                     f'"{data}"')
            contents = mount.read_file(path)
            self.assertEqual(data, contents)
            log.info(f'read perm was tested successfully: "{data}" was '
                     f'successfully read from path {path}')

    def conduct_pos_test_for_write_caps(self):
        filedata = ('some new data on first fs', 'some new data on second fs')

        for mount, path, data in self.test_set:
            log.info(f'test write perm: try writing data "{data}" to '
                     f'file {path}.')
            mount.write_file(path=path, data=data)
            contents = mount.read_file(path=path)
            self.assertEqual(data, contents)
            log.info(f'write perm was tested was successfully: data '
                     f'"{data}" was successfully written to file "{path}".')

    def conduct_neg_test_for_write_caps(self):
        possible_errmsgs = ('permission denied', 'operation not permitted')
        cmdargs = ['echo', 'some random data', Raw('|'), 'tee']

        # don't use data, cmd args to write are set already above.
        for mount, path, data in self.test_set:
            log.info('test absence of write perm: expect failure '
                     f'writing data to file {path}.')
            cmdargs.append(path)
            mount.negtestcmd(args=cmdargs, retval=1,
                             errmsg=possible_errmsgs)
            cmdargs.pop(len(cmdargs)-1)
            log.info('absence of write perm was tested successfully: '
                     f'failed to be write data to file {path}.')

    def get_mon_cap_from_keyring(self, client_name):
        keyring = self.run_cluster_cmd(cmd=f'auth get {client_name}')
        for line in keyring.split('\n'):
            if 'caps mon' in line:
                return line[line.find(' = "') + 4 : -1]

        raise RuntimeError('get_save_mon_cap: mon cap not found in keyring. '
                           'keyring -\n' + keyring)
