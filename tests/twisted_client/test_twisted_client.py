import sys
sys.path.append('beanstalk')
sys.path.append('tests')

import os

from twisted.internet import protocol, reactor, defer
from twisted.internet.task import Clock
from twisted.internet.error import ConnectionDone, ConnectionRefusedError
from twisted.trial import unittest

from twisted_client import Beanstalk, BeanstalkClientFactory, BeanstalkClient
from spawner import spawner
from config import get_config


def _setUp(self):
    config = get_config("ServerConn", "../tests/tests.cfg")
    self.host = config.BEANSTALKD_HOST
    self.port = int(config.BEANSTALKD_PORT)
    spawner.spawn(host=self.host, port=self.port, path=config.BEANSTALKD)


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
        self.client = BeanstalkClient(consumeDisconnects=False, noisy=True)

    def tearDown(self):
        if self.client.protocol:
            d = self.client.disconnect()
        else:
            d = defer.fail("Not connected")
        return d.addCallbacks(self.fail, lambda _: spawner.terminate_all())

    def test_connect_and_disconnect(self):
        self.connected_count = 0
        self.disconnected_count = 0

        def check_connected(client):
            self.connected_count += 1
            self.failUnlessEqual(self.client, client)
            self.failUnless(self.client.protocol)
            return self.client.put("tube", 1).addCallbacks(lambda res: self.failUnlessEqual('ok', res['state']), self.fail)

        def check_disconnected(failure):
            self.disconnected_count += 1
            client, reason = failure.value
            self.failUnlessIsInstance(reason, ConnectionDone)
            self.failIf(self.client.protocol)

        def check_count(_):
            self.failUnlessEqual(1, self.connected_count)
            self.failUnlessEqual(1, self.disconnected_count)

        return self.client.connectTCP(self.host, self.port).addCallbacks(check_connected, self.fail) \
                   .addCallback(lambda _: self.client.disconnect()).addCallbacks(self.fail, check_disconnected) \
                   .addCallback(check_count)

    def test_retry_connect(self):
        self.connected_count = 0
        self.disconnected_count = 0

        def check_connected(client):
            self.connected_count += 1
            self.failUnlessEqual(self.client, client)
            self.failUnless(self.client.protocol)
            return self.client.put("tube", 1).addCallbacks(lambda res: self.failUnlessEqual('ok', res['state']), self.fail)

        def check_disconnected(failure):
            self.disconnected_count += 1
            client, reason = failure.value
            self.failUnlessIsInstance(reason, (ConnectionDone, ConnectionRefusedError))
            self.failIf(self.client.protocol)

        def check_count(_):
            self.failUnlessEqual(1, self.connected_count)
            self.failUnlessEqual(1, self.disconnected_count)

        spawner.terminate_all()
        return self.client.connectTCP(self.host, self.port).addCallbacks(self.fail, check_disconnected) \
                   .addCallback(lambda _: _setUp(self)).addCallback(lambda _: self.client.deferred).addCallbacks(check_connected, self.fail) \
                   .addCallback(check_count)

    def test_reconnect(self):
        self.connected_count = 0
        self.disconnected_count = 0

        def check_connected(client):
            self.connected_count += 1
            self.failUnlessEqual(self.client, client)
            self.failUnless(self.client.protocol)
            return self.client.put("tube", 1).addCallbacks(lambda res: self.failUnlessEqual('ok', res['state']), self.fail)

        def check_disconnected(failure):
            self.disconnected_count += 1
            client, reason = failure.value
            self.failUnlessIsInstance(reason, ConnectionDone)
            self.failIf(self.client.protocol)

        def check_count(_):
            self.failUnlessEqual(2, self.connected_count)
            self.failUnlessEqual(1, self.disconnected_count)

        return self.client.connectTCP(self.host, self.port).addCallbacks(check_connected, self.fail) \
                   .addCallback(lambda _: spawner.terminate_all()).addCallback(lambda _: self.client.deferred).addCallbacks(self.fail, check_disconnected) \
                   .addCallback(lambda _: _setUp(self)).addCallback(lambda _: self.client.deferred).addCallbacks(check_connected, self.fail) \
                   .addCallback(check_count)

    def test_restore_state(self):
        def change_state(_):
            return self.client.use("used") \
                    .addCallback(lambda _: self.client.watch("watched")) \
                    .addCallback(lambda _: self.client.ignore("default"))

        def restart_beanstalkd(_):
            spawner.terminate_all()
            return self.client.deferred.addErrback(lambda _: _setUp(self)).addCallback(lambda _: self.client.deferred)

        def check(_):
            return self.client.list_tube_used().addCallback(lambda res: self.failUnlessEqual("used", res['tube'])) \
                    .addCallback(lambda _: self.client.list_tubes_watched()).addCallback(lambda res: self.failUnlessEqual(["watched"], res['data']))

        return self.client.connectTCP(self.host, self.port).addCallback(change_state).addCallback(restart_beanstalkd).addCallback(check)

    def test_reset_state(self):
        def change_state(_):
            return self.client.use("used") \
                    .addCallback(lambda _: self.client.watch("watched")) \
                    .addCallback(lambda _: self.client.ignore("default"))

        def reconnect(_):
            return self.client.disconnect().addErrback(lambda _: self.client.connectTCP(self.host, self.port))

        def check(_):
            return self.client.list_tube_used().addCallback(lambda res: self.failUnlessEqual("default", res['tube'])) \
                    .addCallback(lambda _: self.client.list_tubes_watched()).addCallback(lambda res: self.failUnlessEqual(["default"], res['data']))

        return self.client.connectTCP(self.host, self.port).addCallback(change_state).addCallback(reconnect).addCallback(check)
