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

    def test_assign_protocol(self):
        f = BeanstalkClientFactory()
        p = f.buildProtocol("abc")
        self.failUnlessEqual(f, p.factory)


class BeanstalkClientTestCase(unittest.TestCase):
    def setUp(self):
        _setUp(self)
        self.client = BeanstalkClient(noisy=True)

    def tearDown(self):
        self.client.disconnect()
        spawner.terminate_all()

    def test_connect_and_disconnect(self):
        self.connected_count = 0
        self.disconnected_count = 0

        def check_connected(client):
            self.connected_count += 1
            self.client.deferred.addCallback(check_disconnected)

            self.failUnlessEqual(self.client, client)
            self.failUnless(self.client.protocol)
            return self.client.protocol.put("tube", 1).addCallback(lambda res: self.failUnlessEqual('ok', res['state']))

        def check_disconnected(client):
            self.disconnected_count += 1
            self.client.deferred.addCallback(lambda _: self.fail(_))

            self.failUnlessEqual(self.client, client)
            self.failIf(self.client.protocol)

        def check_count(_):
            self.failUnlessEqual(1, self.connected_count)
            self.failUnlessEqual(1, self.disconnected_count)

        return self.client.connectTCP(self.host, self.port).addCallback(check_connected) \
                   .addCallback(lambda _: self.client.disconnect()) \
                   .addCallback(check_count)

    def test_retry_connect(self):
        self.connected_count = 0
        self.disconnected_count = 0

        def check_connected(client):
            self.connected_count += 1
            self.client.deferred.addCallback(check_disconnected)

            self.failUnlessEqual(self.client, client)
            self.failUnless(self.client.protocol)
            return self.client.protocol.put("tube", 1).addCallback(lambda res: self.failUnlessEqual('ok', res['state']))

        def check_disconnected(client):
            self.disconnected_count += 1
            self.client.deferred.addCallback(check_connected)

            self.failUnlessEqual(self.client, client)
            self.failIf(self.client.protocol)

        def check_count(_):
            self.failUnlessEqual(1, self.connected_count)
            self.failUnlessEqual(1, self.disconnected_count)

        spawner.terminate_all()
        return self.client.connectTCP(self.host, self.port).addCallback(check_disconnected) \
                   .addCallback(lambda _: _setUp(self)).addCallback(lambda _: self.client.deferred) \
                   .addCallback(check_count)

    def test_reconnect(self):
        self.connected_count = 0
        self.disconnected_count = 0

        def check_connected(client):
            self.connected_count += 1
            self.client.deferred.addCallback(check_disconnected)

            self.failUnlessEqual(self.client, client)
            self.failUnless(self.client.protocol)
            return self.client.protocol.put("tube", 1).addCallback(lambda res: self.failUnlessEqual('ok', res['state']))

        def check_disconnected(client):
            self.disconnected_count += 1
            self.client.deferred.addCallback(check_connected)

            self.failUnlessEqual(self.client, client)
            self.failIf(self.client.protocol)

        def check_count(_):
            self.failUnlessEqual(2, self.connected_count)
            self.failUnlessEqual(1, self.disconnected_count)

        return self.client.connectTCP(self.host, self.port).addCallback(check_connected) \
                   .addCallback(lambda _: spawner.terminate_all()).addCallback(lambda _: self.client.deferred) \
                   .addCallback(lambda _: _setUp(self)).addCallback(lambda _: self.client.deferred) \
                   .addCallback(check_count)

    # def test_connect_when_connected(self):
    #    self.connected_count = 0
    #    self.disconnected_count = 0
    #
    #    def check_connected(proto):
    #        self.connected_count += 1
    #        self.client.deferred.addCallback(check_disconnected)
    #
    #        self.failUnless(proto)
    #        self.failUnlessEqual(self.client.protocol, proto)
    #        return proto.put("tube", 1).addCallback(lambda res: self.failUnlessEqual('ok', res['state']))
    #
    #    def check_disconnected(proto):
    #        self.disconnected_count += 1
    #        self.client.deferred.addCallback(check_connected)
    #
    #        self.failIf(proto)
    #        self.failUnlessEqual(self.client.protocol, proto)
    #
    #    def check_count(_):
    #        self.failUnlessEqual(2, self.connected_count)
    #        self.failUnlessEqual(0, self.disconnected_count)
    #
    #    return self.client.connectTCP(self.host, self.port).addCallback(check_connected) \
    #               .addCallback(lambda _: self.client.connectTCP(self.host, self.port)).addCallback(lambda _: self.client.deferred) \
    #               .addCallback(check_count)
