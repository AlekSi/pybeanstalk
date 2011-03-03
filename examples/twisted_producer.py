#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

import beanstalk

from twisted.python import log
log.startLogging(sys.stdout)

def worker(client):
    bs = client.protocol
    bs.use("myqueue")
    bs.put('Look!  A job!', 8192, 0, 300) \
      .addCallback(lambda x: sys.stdout.write("Queued job: %s\n" % `x`)) \
      .addCallback(lambda _: reactor.stop())

client = beanstalk.twisted_client.BeanstalkClient(noisy=True)
d = client.connectTCP(sys.argv[1], 11300)
d.addCallback(worker)
reactor.run()
