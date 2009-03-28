# --------------------------------------------------------
# Imports
# --------------------------------------------------------
from unittest import TestCase

from dynamite.load_balancer.load_balancer import LoadBalancer

# --------------------------------------------------------
# Test
# --------------------------------------------------------
class TestLoadBalancer(TestCase):
    def test_empty_server_list(self):
        """
        Ensures the load balancer throws an exception when given
        an empty server list.
        """
        threw_exc = False
        try:
            load_balancer = LoadBalancer([], 20000)
        except:
            threw_exc = True
        self.assertTrue(threw_exc)