#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

import beanstalk

from twisted.python import log
log.startLogging(sys.stdout)

def prepare(client):
    client.watch("demo")
    client.ignore("default")
    return client

def worker(client):
    def execute(job):
        log.msg("Got job: %r" % job)
        client.touch(job['jid'])
        return client.delete(job['jid'])

    return client.reserve().addCallback(execute).addCallback(lambda _: reactor.callLater(0, worker, client))

client = beanstalk.twisted_client.BeanstalkClient(noisy=True)
client.connectTCP(sys.argv[1], 11300).addCallback(prepare).addCallback(worker)
reactor.run()
