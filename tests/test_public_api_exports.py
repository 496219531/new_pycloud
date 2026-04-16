from pycloud_parallel import DataRef as TopLevelDataRef
from pycloud_parallel import JobQueue as TopLevelJobQueue
from pycloud_parallel import Service as TopLevelService
from pycloud_parallel import TaskPool as TopLevelTaskPool
from pycloud_parallel import export as TopLevelExport
from pycloud_parallel.api.common import DataRef as ApiDataRef
from pycloud_parallel.api.common import export as ApiExport
from pycloud_parallel.api.pool import TaskPool as ApiTaskPool
from pycloud_parallel.api.queue import JobQueue as ApiJobQueue
from pycloud_parallel.api.service import Service as ApiService
from pycloud_parallel.controlplane import Artifact as ControlplaneArtifact
from pycloud_parallel.controlplane import ArtifactDeps as ControlplaneArtifactDeps
from pycloud_parallel.controlplane import ArtifactExports as ControlplaneArtifactExports
from pycloud_parallel.controlplane import DataRef as ControlplaneDataRef
from pycloud_parallel.controlplane.client import Artifact as ClientArtifact
from pycloud_parallel.controlplane.client import ArtifactDeps as ClientArtifactDeps
from pycloud_parallel.controlplane.client import ArtifactExports as ClientArtifactExports
from pycloud_parallel.controlplane.client import DataRef as ClientDataRef
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def test_top_level_v1_surface_reexports_api_classes():
    assert TopLevelService is ApiService
    assert TopLevelTaskPool is ApiTaskPool
    assert TopLevelJobQueue is ApiJobQueue
    assert TopLevelDataRef is ApiDataRef
    assert TopLevelExport is ApiExport


def test_controlplane_artifact_reexports_client_class():
    assert ControlplaneArtifact is ClientArtifact
    assert ControlplaneArtifactDeps is ClientArtifactDeps
    assert ControlplaneArtifactExports is ClientArtifactExports
    assert ControlplaneDataRef is ClientDataRef


def test_proto_messages_expose_node_instance_id_fields():
    assert "node_instance_id" in pb2.RegisterNodeRequest.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.RegisterNodeResponse.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.HeartbeatNodeRequest.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.NodeInfo.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.ServiceRouteInfo.DESCRIPTOR.fields_by_name
