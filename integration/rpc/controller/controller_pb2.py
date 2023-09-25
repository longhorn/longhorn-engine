# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: controller.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
import common_pb2 as common__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10\x63ontroller.proto\x12\x06ptypes\x1a\x1bgoogle/protobuf/empty.proto\x1a\x0c\x63ommon.proto\"\xf1\x01\n\x06Volume\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0c\n\x04size\x18\x02 \x01(\x03\x12\x14\n\x0creplicaCount\x18\x03 \x01(\x05\x12\x10\n\x08\x65ndpoint\x18\x04 \x01(\t\x12\x10\n\x08\x66rontend\x18\x05 \x01(\t\x12\x15\n\rfrontendState\x18\x06 \x01(\t\x12\x13\n\x0bisExpanding\x18\x07 \x01(\x08\x12\x1c\n\x14last_expansion_error\x18\x08 \x01(\t\x12 \n\x18last_expansion_failed_at\x18\t \x01(\t\x12%\n\x1dunmap_mark_snap_chain_removed\x18\n \x01(\x08\"7\n\x0eReplicaAddress\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t\x12\x14\n\x0cinstanceName\x18\x02 \x01(\t\"_\n\x11\x43ontrollerReplica\x12\'\n\x07\x61\x64\x64ress\x18\x01 \x01(\x0b\x32\x16.ptypes.ReplicaAddress\x12!\n\x04mode\x18\x02 \x01(\x0e\x32\x13.ptypes.ReplicaMode\"Q\n\x12VolumeStartRequest\x12\x18\n\x10replicaAddresses\x18\x01 \x03(\t\x12\x0c\n\x04size\x18\x02 \x01(\x03\x12\x13\n\x0b\x63urrentSize\x18\x03 \x01(\x03\"\x8f\x01\n\x15VolumeSnapshotRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x39\n\x06labels\x18\x02 \x03(\x0b\x32).ptypes.VolumeSnapshotRequest.LabelsEntry\x1a-\n\x0bLabelsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"#\n\x13VolumeSnapshotReply\x12\x0c\n\x04name\x18\x01 \x01(\t\"#\n\x13VolumeRevertRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\"#\n\x13VolumeExpandRequest\x12\x0c\n\x04size\x18\x01 \x01(\x03\".\n\x1aVolumeFrontendStartRequest\x12\x10\n\x08\x66rontend\x18\x01 \x01(\t\"<\n)VolumeUnmapMarkSnapChainRemovedSetRequest\x12\x0f\n\x07\x65nabled\x18\x01 \x01(\x08\"3\n\x1bVolumePrepareRestoreRequest\x12\x14\n\x0clastRestored\x18\x01 \x01(\t\"5\n\x1aVolumeFinishRestoreRequest\x12\x17\n\x0f\x63urrentRestored\x18\x01 \x01(\t\"?\n\x10ReplicaListReply\x12+\n\x08replicas\x18\x01 \x03(\x0b\x32\x19.ptypes.ControllerReplica\"o\n\x1e\x43ontrollerReplicaCreateRequest\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t\x12\x19\n\x11snapshot_required\x18\x02 \x01(\x08\x12!\n\x04mode\x18\x03 \x01(\x0e\x32\x13.ptypes.ReplicaMode\"{\n\x1aReplicaPrepareRebuildReply\x12*\n\x07replica\x18\x01 \x01(\x0b\x32\x19.ptypes.ControllerReplica\x12\x31\n\x13sync_file_info_list\x18\x02 \x03(\x0b\x32\x14.ptypes.SyncFileInfo\"#\n\x12JournalListRequest\x12\r\n\x05limit\x18\x01 \x01(\x03\"\xef\x01\n\rVersionOutput\x12\x0f\n\x07version\x18\x01 \x01(\t\x12\x11\n\tgitCommit\x18\x02 \x01(\t\x12\x11\n\tbuildDate\x18\x03 \x01(\t\x12\x15\n\rcliAPIVersion\x18\x04 \x01(\x03\x12\x18\n\x10\x63liAPIMinVersion\x18\x05 \x01(\x03\x12\x1c\n\x14\x63ontrollerAPIVersion\x18\x06 \x01(\x03\x12\x1f\n\x17\x63ontrollerAPIMinVersion\x18\x07 \x01(\x03\x12\x19\n\x11\x64\x61taFormatVersion\x18\x08 \x01(\x03\x12\x1c\n\x14\x64\x61taFormatMinVersion\x18\t \x01(\x03\"?\n\x15VersionDetailGetReply\x12&\n\x07version\x18\x01 \x01(\x0b\x32\x15.ptypes.VersionOutput\"\x8a\x01\n\x07Metrics\x12\x16\n\x0ereadThroughput\x18\x01 \x01(\x04\x12\x17\n\x0fwriteThroughput\x18\x02 \x01(\x04\x12\x13\n\x0breadLatency\x18\x03 \x01(\x04\x12\x14\n\x0cwriteLatency\x18\x04 \x01(\x04\x12\x10\n\x08readIOPS\x18\x05 \x01(\x04\x12\x11\n\twriteIOPS\x18\x06 \x01(\x04\"3\n\x0fMetricsGetReply\x12 \n\x07metrics\x18\x01 \x01(\x0b\x32\x0f.ptypes.Metrics*&\n\x0bReplicaMode\x12\x06\n\x02WO\x10\x00\x12\x06\n\x02RW\x10\x01\x12\x07\n\x03\x45RR\x10\x02\x32\xd0\n\n\x11\x43ontrollerService\x12\x33\n\tVolumeGet\x12\x16.google.protobuf.Empty\x1a\x0e.ptypes.Volume\x12\x39\n\x0bVolumeStart\x12\x1a.ptypes.VolumeStartRequest\x1a\x0e.ptypes.Volume\x12\x38\n\x0eVolumeShutdown\x12\x16.google.protobuf.Empty\x1a\x0e.ptypes.Volume\x12L\n\x0eVolumeSnapshot\x12\x1d.ptypes.VolumeSnapshotRequest\x1a\x1b.ptypes.VolumeSnapshotReply\x12;\n\x0cVolumeRevert\x12\x1b.ptypes.VolumeRevertRequest\x1a\x0e.ptypes.Volume\x12;\n\x0cVolumeExpand\x12\x1b.ptypes.VolumeExpandRequest\x1a\x0e.ptypes.Volume\x12I\n\x13VolumeFrontendStart\x12\".ptypes.VolumeFrontendStartRequest\x1a\x0e.ptypes.Volume\x12@\n\x16VolumeFrontendShutdown\x12\x16.google.protobuf.Empty\x1a\x0e.ptypes.Volume\x12g\n\"VolumeUnmapMarkSnapChainRemovedSet\x12\x31.ptypes.VolumeUnmapMarkSnapChainRemovedSetRequest\x1a\x0e.ptypes.Volume\x12?\n\x0bReplicaList\x12\x16.google.protobuf.Empty\x1a\x18.ptypes.ReplicaListReply\x12?\n\nReplicaGet\x12\x16.ptypes.ReplicaAddress\x1a\x19.ptypes.ControllerReplica\x12\\\n\x17\x43ontrollerReplicaCreate\x12&.ptypes.ControllerReplicaCreateRequest\x1a\x19.ptypes.ControllerReplica\x12?\n\rReplicaDelete\x12\x16.ptypes.ReplicaAddress\x1a\x16.google.protobuf.Empty\x12\x45\n\rReplicaUpdate\x12\x19.ptypes.ControllerReplica\x1a\x19.ptypes.ControllerReplica\x12S\n\x15ReplicaPrepareRebuild\x12\x16.ptypes.ReplicaAddress\x1a\".ptypes.ReplicaPrepareRebuildReply\x12I\n\x14ReplicaVerifyRebuild\x12\x16.ptypes.ReplicaAddress\x1a\x19.ptypes.ControllerReplica\x12\x41\n\x0bJournalList\x12\x1a.ptypes.JournalListRequest\x1a\x16.google.protobuf.Empty\x12I\n\x10VersionDetailGet\x12\x16.google.protobuf.Empty\x1a\x1d.ptypes.VersionDetailGetReply\x12=\n\nMetricsGet\x12\x16.google.protobuf.Empty\x1a\x17.ptypes.MetricsGetReplyb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'controller_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _VOLUMESNAPSHOTREQUEST_LABELSENTRY._options = None
  _VOLUMESNAPSHOTREQUEST_LABELSENTRY._serialized_options = b'8\001'
  _globals['_REPLICAMODE']._serialized_start=1868
  _globals['_REPLICAMODE']._serialized_end=1906
  _globals['_VOLUME']._serialized_start=72
  _globals['_VOLUME']._serialized_end=313
  _globals['_REPLICAADDRESS']._serialized_start=315
  _globals['_REPLICAADDRESS']._serialized_end=370
  _globals['_CONTROLLERREPLICA']._serialized_start=372
  _globals['_CONTROLLERREPLICA']._serialized_end=467
  _globals['_VOLUMESTARTREQUEST']._serialized_start=469
  _globals['_VOLUMESTARTREQUEST']._serialized_end=550
  _globals['_VOLUMESNAPSHOTREQUEST']._serialized_start=553
  _globals['_VOLUMESNAPSHOTREQUEST']._serialized_end=696
  _globals['_VOLUMESNAPSHOTREQUEST_LABELSENTRY']._serialized_start=651
  _globals['_VOLUMESNAPSHOTREQUEST_LABELSENTRY']._serialized_end=696
  _globals['_VOLUMESNAPSHOTREPLY']._serialized_start=698
  _globals['_VOLUMESNAPSHOTREPLY']._serialized_end=733
  _globals['_VOLUMEREVERTREQUEST']._serialized_start=735
  _globals['_VOLUMEREVERTREQUEST']._serialized_end=770
  _globals['_VOLUMEEXPANDREQUEST']._serialized_start=772
  _globals['_VOLUMEEXPANDREQUEST']._serialized_end=807
  _globals['_VOLUMEFRONTENDSTARTREQUEST']._serialized_start=809
  _globals['_VOLUMEFRONTENDSTARTREQUEST']._serialized_end=855
  _globals['_VOLUMEUNMAPMARKSNAPCHAINREMOVEDSETREQUEST']._serialized_start=857
  _globals['_VOLUMEUNMAPMARKSNAPCHAINREMOVEDSETREQUEST']._serialized_end=917
  _globals['_VOLUMEPREPARERESTOREREQUEST']._serialized_start=919
  _globals['_VOLUMEPREPARERESTOREREQUEST']._serialized_end=970
  _globals['_VOLUMEFINISHRESTOREREQUEST']._serialized_start=972
  _globals['_VOLUMEFINISHRESTOREREQUEST']._serialized_end=1025
  _globals['_REPLICALISTREPLY']._serialized_start=1027
  _globals['_REPLICALISTREPLY']._serialized_end=1090
  _globals['_CONTROLLERREPLICACREATEREQUEST']._serialized_start=1092
  _globals['_CONTROLLERREPLICACREATEREQUEST']._serialized_end=1203
  _globals['_REPLICAPREPAREREBUILDREPLY']._serialized_start=1205
  _globals['_REPLICAPREPAREREBUILDREPLY']._serialized_end=1328
  _globals['_JOURNALLISTREQUEST']._serialized_start=1330
  _globals['_JOURNALLISTREQUEST']._serialized_end=1365
  _globals['_VERSIONOUTPUT']._serialized_start=1368
  _globals['_VERSIONOUTPUT']._serialized_end=1607
  _globals['_VERSIONDETAILGETREPLY']._serialized_start=1609
  _globals['_VERSIONDETAILGETREPLY']._serialized_end=1672
  _globals['_METRICS']._serialized_start=1675
  _globals['_METRICS']._serialized_end=1813
  _globals['_METRICSGETREPLY']._serialized_start=1815
  _globals['_METRICSGETREPLY']._serialized_end=1866
  _globals['_CONTROLLERSERVICE']._serialized_start=1909
  _globals['_CONTROLLERSERVICE']._serialized_end=3269
# @@protoc_insertion_point(module_scope)
