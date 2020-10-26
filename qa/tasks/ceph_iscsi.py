"""
Run ceph-iscsi cluster setup
"""
import logging
import contextlib
from io import StringIO
from teuthology.exceptions import CommandFailedError, ConnectionLostError
from teuthology.orchestra import run
from textwrap import dedent

log = logging.getLogger(__name__)

class IscsiSetup(object):
    def __init__(self, ctx, config):
        self.ctx = ctx
        self.config = config
        self.target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:ceph-gw"
        self.client_iqn = "iqn.1994-05.com.redhat:client"
        self.trusted_ip_list = []
        self.background_procs = []

    def run_daemon(self, remote, cmds):
        p = remote.run(args=['sudo', 'adjust-ulimits', 'daemon-helper', 'kill', cmds],
                          wait=False, stdin=run.PIPE, stdout=StringIO())
        self.background_procs.append(p)

    def _kill_background(self, p):
        if p.stdin:
            p.stdin.close()
            try:
                p.wait()
            except (CommandFailedError, ConnectionLostError):
                pass

    def kill_backgrounds(self):
        for p in self.background_procs:
            self._kill_background(p)
        self.background_procs = []

    def _setup_gateway(self, role):
        """Spawned task that setups the gateway"""
        (remote,) = self.ctx.cluster.only(role).remotes.keys()

        # setup the iscsi-gateway.cfg file, we only set the
        # clust_name and trusted_ip_list and all the others
        # as default
        target = "/etc/ceph/iscsi-gateway.cfg"
        args = ['sudo', 'echo', 'cluster_name = ceph', run.Raw('>'), target]
        remote.run(args=args)
        args = ['sudo', 'echo', 'pool = rbd', run.Raw('>>'), target]
        remote.run(args=args)
        args = ['sudo', 'echo', 'api_secure = false', run.Raw('>>'), target]
        remote.run(args=args)
        args = ['sudo', 'echo', 'api_port = 5000', run.Raw('>>'), target]
        remote.run(args=args)
        ips = ','.join(self.trusted_ip_list)
        args = ['sudo', 'echo', 'trusted_ip_list = {}'.format(ips), run.Raw('>>'), target]
        remote.run(args=args)

        self.run_daemon(remote, "/usr/bin/tcmu-runner")
        self.run_daemon(remote, "/usr/bin/rbd-target-gw")
        self.run_daemon(remote, "/usr/bin/rbd-target-api")

    def setup_gateways(self):
        for role in self.config['gateways']:
            (remote,) = self.ctx.cluster.only(role).remotes.keys()
            self.trusted_ip_list.append(remote.ip_address)

        for role in self.config['gateways']:
            self._setup_gateway(remote)

    def _setup_client(self, role):
        """Spawned task that setups the gateway"""
        (remote,) = self.ctx.cluster.only(role).remotes.keys()

        target = "/etc/iscsi/initiatorname.iscsi"
        remote.run(args=['sudo', 'echo', f'InitiatorName={self.client_iqn}', run.Raw('>'), target])
        # the restart is needed after the above change is applied
        remote.run(args=['sudo', 'systemctl', 'restart', 'iscsid'])

        remote.run(args=['sudo', 'modprobe', 'dm_multipath'])
        remote.run(args=['sudo', 'mpathconf', '--enable'])
        conf = dedent('''\
devices {
        device {
                vendor                 "LIO-ORG"
                product                "LIO-ORG"
                hardware_handler       "1 alua"
                path_grouping_policy   "failover"
                path_selector          "queue-length 0"
                failback               60
                path_checker           tur
                prio                   alua
                prio_args              exclusive_pref_bit
                fast_io_fail_tmo       25
                no_path_retry          queue
        }
}
''')
        path = "/etc/multipath.conf"
        remote.sudo_write_file(path, conf, append=True)
        remote.run(args=['sudo', 'systemctl', 'start', 'multipathd'])

    def setup_clients(self):
        for role in self.config['clients']:
            self._setup_client(role)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run ceph iscsi setup.

    Specify the list of gateways to run ::

      tasks:
        ceph_iscsi:
          gateways: [gateway.0, gateway.1]
          clients: [client.0]

    """
    log.info('Setting ceph iscsi cluster...')
    iscsi = IscsiSetup(ctx, config)
    iscsi.setup_gateways()
    iscsi.setup_clients()

    try:
        yield
    finally:
        log.info('Ending ceph iscsi daemons')
        iscsi.kill_backgrounds()
