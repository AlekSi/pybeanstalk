import sys
sys.path.append('beanstalk')
sys.path.append('tests')

import os

from twisted.internet import protocol, reactor
from twisted.trial import unittest

from twisted_client import Beanstalk
from spawner import spawner
from config import get_config

class BTestCase(unittest.TestCase):
    def setUp(self):
        config = get_config("ServerConn", "../tests/tests.cfg")
        self.host = config.BEANSTALKD_HOST
        self.port = config.BEANSTALKD_PORT
        self.path = os.path.join(config.BPATH, config.BEANSTALKD)
        spawner.spawn(host=self.host, port=self.port, path=self.path)

    def tearDown(self):
        spawner.terminate_all()

    def test_omg(self):
        return protocol.ClientCreator(reactor, Beanstalk).connectTCP(self.host, int(self.port))
