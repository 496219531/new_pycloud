from pycloud_parallel import DataRef as TopLevelDataRef
from pycloud_parallel import JobQueue as TopLevelJobQueue
from pycloud_parallel import Service as TopLevelService
from pycloud_parallel import TaskPool as TopLevelTaskPool
from pycloud_parallel import export as TopLevelExport
from pycloud_parallel.api.common import DataRef as ApiDataRef
from pycloud_parallel.api.common import export as ApiExport
from pycloud_parallel.artifact import Artifact as PublicArtifact
from pycloud_parallel.artifact import ArtifactDeps as PublicArtifactDeps
from pycloud_parallel.artifact import ArtifactExports as PublicArtifactExports
from pycloud_parallel.api.pool import TaskPool as ApiTaskPool
from pycloud_parallel.api.queue import JobQueue as ApiJobQueue
from pycloud_parallel.api.service import Service as ApiService
from pycloud_parallel.controlplane.artifact import Artifact as ControlplaneArtifact
from pycloud_parallel.controlplane.artifact import ArtifactDeps as ControlplaneArtifactDeps
from pycloud_parallel.controlplane.artifact import ArtifactExports as ControlplaneArtifactExports
from pycloud_parallel.data.ref import DataRef as ControlplaneDataRef
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def test_top_level_v1_surface_reexports_api_classes():
    assert TopLevelService is ApiService
    assert TopLevelTaskPool is ApiTaskPool
    assert TopLevelJobQueue is ApiJobQueue
    assert TopLevelDataRef is ApiDataRef
    assert TopLevelExport is ApiExport


def test_controlplane_artifact_and_data_modules_are_direct_authority():
    assert ControlplaneArtifact.__module__ == "pycloud_parallel.controlplane.artifact"
    assert ControlplaneArtifactDeps.__module__ == "pycloud_parallel.controlplane.artifact"
    assert ControlplaneArtifactExports.__module__ == "pycloud_parallel.controlplane.artifact"
    assert ControlplaneDataRef.__module__ == "pycloud_parallel.data.ref"


def test_artifact_package_exposes_advanced_artifact_api_without_top_level_export():
    assert PublicArtifact is ControlplaneArtifact
    assert PublicArtifactDeps is ControlplaneArtifactDeps
    assert PublicArtifactExports is ControlplaneArtifactExports


def test_proto_messages_expose_node_instance_id_fields():
    assert "node_instance_id" in pb2.RegisterNodeRequest.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.RegisterNodeResponse.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.HeartbeatNodeRequest.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.NodeInfo.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.ServiceRouteInfo.DESCRIPTOR.fields_by_name
    assert "capability" in pb2.RegisterNodeRequest.DESCRIPTOR.fields_by_name
    assert "capability" in pb2.HeartbeatNodeRequest.DESCRIPTOR.fields_by_name
    assert "capability" in pb2.NodeInfo.DESCRIPTOR.fields_by_name
    assert "capability" in pb2.ServiceRouteInfo.DESCRIPTOR.fields_by_name
    assert "policy_id" in pb2.ServiceRouteReport.DESCRIPTOR.fields_by_name
    assert "policy_id" in pb2.ServiceRouteInfo.DESCRIPTOR.fields_by_name
    assert "policy_id" in pb2.CreateServiceMeta.DESCRIPTOR.fields_by_name
    assert "policy_id" in pb2.CreateServiceResponse.DESCRIPTOR.fields_by_name
    assert "policy_id" in pb2.ServiceStatusInfo.DESCRIPTOR.fields_by_name
