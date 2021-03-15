.. _cephadm-adoption:

Converting an existing cluster to cephadm
=========================================

It is possible to convert some existing clusters so that they can be managed
with ``cephadm``. This statment applies to some clusters that were deployed
with ``ceph-deploy``, ``ceph-ansible``, or ``DeepSea``.

This section of the documentation explains how to determine whether your
clusters can be converted to a state in which they can be managed by
``cephadm`` and how to perform those conversions.

Limitations
-----------

* Cephadm works only with BlueStore OSDs. FileStore OSDs that are in your
  cluster cannot be managed with ``cephadm``.

Preparation
-----------

#. Get the ``cephadm`` command line tool on each host in the existing
   cluster.  See :ref:`get-cephadm`.

#. Prepare each host for use by ``cephadm``:

   .. prompt:: bash #

      cephadm prepare-host

#. Determine which Ceph version you will use.  You can use any Octopus (15.2.z)
   release or later.  For example, ``docker.io/ceph/ceph:v15.2.0``.  The default
   will be the latest stable release, but if you are upgrading from an earlier
   release at the same time be sure to refer to the upgrade notes for any
   special steps to take while upgrading.

   The image is passed to cephadm with:

   .. prompt:: bash #

      cephadm --image $IMAGE <rest of command goes here>

#. Cephadm can provide a list of all Ceph daemons on the current host:

   .. prompt:: bash #

      cephadm ls

   Before starting, you should see that all existing daemons have a
   style of ``legacy`` in the resulting output.  As the adoption
   process progresses, adopted daemons will appear as style
   ``cephadm:v1``.


Adoption process
----------------

#. Ensure the ceph configuration is migrated to use the cluster config database.
   If the ``/etc/ceph/ceph.conf`` is identical on each host, then on one host:

   .. prompt:: bash #

      ceph config assimilate-conf -i /etc/ceph/ceph.conf

   If there are config variations on each host, you may need to repeat
   this command on each host.  You can view the cluster's
   configuration to confirm that it is complete with:

   .. prompt:: bash #

      ceph config dump

#. Adopt each monitor:

   .. prompt:: bash #

      cephadm adopt --style legacy --name mon.<hostname>

   Each legacy monitor should stop, quickly restart as a cephadm
   container, and rejoin the quorum.

#. Adopt each manager:

   .. prompt:: bash #

      cephadm adopt --style legacy --name mgr.<hostname>

#. Enable cephadm:

   .. prompt:: bash #

      ceph mgr module enable cephadm
      ceph orch set backend cephadm

#. Generate an SSH key:

   .. prompt:: bash #

      ceph cephadm generate-key
      ceph cephadm get-pub-key > ~/ceph.pub

#. Install the cluster SSH key on each host in the cluster:

   .. prompt:: bash #

      ssh-copy-id -f -i ~/ceph.pub root@<host>

   .. note::
     It is also possible to import an existing ssh key. See
     :ref:`ssh errors <cephadm-ssh-errors>` in the troubleshooting
     document for instructions describing how to import existing
     ssh keys.

#. Tell cephadm which hosts to manage:

   .. prompt:: bash #

      ceph orch host add <hostname> [ip-address]

   This will perform a ``cephadm check-host`` on each host before
   adding it to ensure it is working.  The IP address argument is only
   required if DNS does not allow you to connect to each host by its
   short name.

#. Verify that the adopted monitor and manager daemons are visible:

   .. prompt:: bash #

      ceph orch ps

#. Adopt all OSDs in the cluster:

   .. prompt:: bash #

      cephadm adopt --style legacy --name <name>

   For example:

   .. prompt:: bash #

      cephadm adopt --style legacy --name osd.1
      cephadm adopt --style legacy --name osd.2

#. Redeploy MDS daemons by telling cephadm how many daemons to run for
   each file system.  You can list file systems by name with ``ceph fs
   ls``.  Run the following command on the master nodes:

   .. prompt:: bash #

      ceph orch apply mds <fs-name> [--placement=<placement>]

   For example, in a cluster with a single file system called `foo`:

   .. prompt:: bash #

      ceph fs ls

   .. code-block:: bash

      name: foo, metadata pool: foo_metadata, data pools: [foo_data ]

   .. prompt:: bash #

      ceph orch apply mds foo 2

   Wait for the new MDS daemons to start with:

   .. prompt:: bash #

      ceph orch ps --daemon-type mds

   Finally, stop and remove the legacy MDS daemons:

   .. prompt:: bash #

      systemctl stop ceph-mds.target
      rm -rf /var/lib/ceph/mds/ceph-*

#. Redeploy RGW daemons.  Cephadm manages RGW daemons by zone.  For each
   zone, deploy new RGW daemons with cephadm:

   .. prompt:: bash #

      ceph orch apply rgw <realm> <zone> [--subcluster=<subcluster>] [--port=<port>] [--ssl] [--placement=<placement>]

   where *<placement>* can be a simple daemon count, or a list of
   specific hosts (see :ref:`orchestrator-cli-placement-spec`).

   Once the daemons have started and you have confirmed they are functioning,
   stop and remove the old legacy daemons:

   .. prompt:: bash #

      systemctl stop ceph-rgw.target
      rm -rf /var/lib/ceph/radosgw/ceph-*

   For adopting single-site systems without a realm, see also
   :ref:`rgw-multisite-migrate-from-single-site`.

#. Check the ``ceph health detail`` output for cephadm warnings about
   stray cluster daemons or hosts that are not yet managed.
