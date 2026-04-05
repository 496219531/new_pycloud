from pycloud_parallel import GatewayConnect as TopLevelGatewayConnect
from pycloud_parallel.controlplane import GatewayConnect as ControlplaneGatewayConnect
from pycloud_parallel.controlplane.client import GatewayConnect as ClientGatewayConnect


def test_top_level_gateway_connect_reexports_client_class():
    assert TopLevelGatewayConnect is ClientGatewayConnect


def test_controlplane_gateway_connect_reexports_client_class():
    assert ControlplaneGatewayConnect is ClientGatewayConnect
