import sys
sys.path.append('beanstalk')
sys.path.append('tests')

import os

from twisted.internet import protocol, reactor
from twisted.internet.task import Clock
from twisted.trial import unittest

from twisted_client import Beanstalk, BeanstalkClientFactory, BeanstalkClient
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
        def check(proto):
            self.failUnless(proto)
            return proto.put("tube", 1).addCallback(lambda res: self.failUnlessEqual('ok', res['state']))

        return protocol.ClientCreator(reactor, Beanstalk).connectTCP(self.host, self.port).addCallback(check)


class BeanstalkClientFactoryTestCase(unittest.TestCase):
    def setUp(self):
        _setUp(self)

    def tearDown(self):
        spawner.terminate_all()

    def test_assing_protocol(self):
        f = BeanstalkClientFactory()
        p = f.buildProtocol("abc")
        self.failUnlessEqual(f, p.factory)


class BeanstalkClientTestCase(unittest.TestCase):
    def setUp(self):
        _setUp(self)
        self.client = BeanstalkClient()
        self.client.noisy = True
        self.client.clock = Clock()

    def tearDown(self):
        spawner.terminate_all()

    def test_simplest(self):
        def check(proto):
            self.failUnless(proto)
            self.failUnlessEqual(self.client.protocol, proto)
            return proto.put("tube", 1).addCallback(lambda res: self.failUnlessEqual('ok', res['state']))

        d = self.client.connectTCP(self.host, self.port).addCallback(check)
        self.client.clock.advance(1)
        return d

    # def test_reconnect(self):
    #     def check(proto):
    #         self.failUnless(proto)
    #         self.failUnlessEqual(self.client.protocol, proto)
    #         return proto.put("tube", 1).addCallback(lambda res: self.failUnlessEqual('ok', res['state']))
    # 
    #     spawner.terminate_all()
    #     reactor.callLater(1, _setUp, self)
    #     return BeanstalkClient.connectTCP(self.host, self.port).addCallback(check)
