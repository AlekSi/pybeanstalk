import sys
sys.path.append('beanstalk')
sys.path.append('tests')

import os

from twisted.internet import protocol, reactor
from twisted.trial import unittest

from twisted_client import Beanstalk, BeanstalkClientFactory
from spawner import spawner
from config import get_config


def _setUp(self):
    config = get_config("ServerConn", "../tests/tests.cfg")
    self.host = config.BEANSTALKD_HOST
    self.port = int(config.BEANSTALKD_PORT)
    self.path = os.path.join(config.BPATH, config.BEANSTALKD)
    spawner.spawn(host=self.host, port=self.port, path=self.path)


class BeanstalkTestCase(unittest.TestCase):
    def setUp(self):
        _setUp(self)

    def tearDown(self):
        spawner.terminate_all()

    def test_simplest(self):
        return protocol.ClientCreator(reactor, Beanstalk).connectTCP(self.host, self.port) \
                       .addCallback(lambda proto: proto.put("tube", 1)) \
                       .addCallback(lambda res: self.failUnlessEqual('ok', res['state']))


class BeanstalkClientFactoryTestCase(unittest.TestCase):
    def setUp(self):
        _setUp(self)

    def tearDown(self):
        spawner.terminate_all()

    def test_assing_protocol(self):
        f = BeanstalkClientFactory()
        p = f.buildProtocol("abc")
        self.failUnlessEqual(f, p.factory)
