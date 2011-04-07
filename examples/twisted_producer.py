#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

import beanstalk

from twisted.python import log
log.startLogging(sys.stdout)

i = 0

def worker(client):
    global i
    i += 1
    body = 'Job %d' % i
    client.put(body).addCallback(lambda res: log.msg("%s: %s" % (body, res)))
    if not client.protocol:
        log.msg("%s will be put on connect" % body)

client = beanstalk.twisted_client.BeanstalkClient(noisy=True)
client.connectTCP(sys.argv[1], 11300)
client.use("demo")
task.LoopingCall(worker, client).start(2, now=False)
reactor.run()
