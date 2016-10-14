# go-iscsi-helper
Helper function for iSCSI

Require TGT to be installed, as well as open-iscsi.

If used in container, open-iscsi need to be installed on the host.

This library is good enough for:
1. Start/stop a iscsi target using tgt, with certain backstores. This can be
   done inside container.
2. Start/stop a iscsi initiator on the host, connect to a target then create the
   device.
