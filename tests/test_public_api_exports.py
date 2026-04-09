from pycloud_parallel import GatewayConnect as TopLevelGatewayConnect
from pycloud_parallel.controlplane import GatewayConnect as ControlplaneGatewayConnect
from pycloud_parallel.controlplane.client import GatewayConnect as ClientGatewayConnect
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


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
