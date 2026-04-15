from pycloud_parallel import Artifact as TopLevelArtifact
from pycloud_parallel import ArtifactDeps as TopLevelArtifactDeps
from pycloud_parallel import ArtifactExports as TopLevelArtifactExports
from pycloud_parallel import DataRef as TopLevelDataRef
from pycloud_parallel import GatewayConnect as TopLevelGatewayConnect
from pycloud_parallel.controlplane import Artifact as ControlplaneArtifact
from pycloud_parallel.controlplane import ArtifactDeps as ControlplaneArtifactDeps
from pycloud_parallel.controlplane import ArtifactExports as ControlplaneArtifactExports
from pycloud_parallel.controlplane import DataRef as ControlplaneDataRef
from pycloud_parallel.controlplane import GatewayConnect as ControlplaneGatewayConnect
from pycloud_parallel.controlplane.client import Artifact as ClientArtifact
from pycloud_parallel.controlplane.client import ArtifactDeps as ClientArtifactDeps
from pycloud_parallel.controlplane.client import ArtifactExports as ClientArtifactExports
from pycloud_parallel.controlplane.client import DataRef as ClientDataRef
from pycloud_parallel.controlplane.client import GatewayConnect as ClientGatewayConnect
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def test_top_level_artifact_reexports_client_class():
    assert TopLevelArtifact is ClientArtifact
    assert TopLevelArtifactDeps is ClientArtifactDeps
    assert TopLevelArtifactExports is ClientArtifactExports
    assert TopLevelDataRef is ClientDataRef


def test_controlplane_artifact_reexports_client_class():
    assert ControlplaneArtifact is ClientArtifact
    assert ControlplaneArtifactDeps is ClientArtifactDeps
    assert ControlplaneArtifactExports is ClientArtifactExports
    assert ControlplaneDataRef is ClientDataRef


def test_top_level_gateway_connect_reexports_client_class():
    assert TopLevelGatewayConnect is ClientGatewayConnect


def test_controlplane_gateway_connect_reexports_client_class():
    assert ControlplaneGatewayConnect is ClientGatewayConnect


def test_proto_messages_expose_node_instance_id_fields():
    assert "node_instance_id" in pb2.RegisterNodeRequest.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.RegisterNodeResponse.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.HeartbeatNodeRequest.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.NodeInfo.DESCRIPTOR.fields_by_name
    assert "node_instance_id" in pb2.ServiceRouteInfo.DESCRIPTOR.fields_by_name
