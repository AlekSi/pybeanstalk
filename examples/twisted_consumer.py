#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

import beanstalk

from twisted.python import log
log.startLogging(sys.stdout)

def executor(bs, jobdata):
    log.msg("Running job %s" % `jobdata`)
    bs.touch(jobdata['jid'])
    bs.delete(jobdata['jid'])

def error_handler(e):
    print "Got an error", e

def executionGenerator(bs):
    while True:
        log.msg("Waiting for a job...")
        yield bs.reserve().addCallback(lambda v: executor(bs, v)).addErrback(error_handler)

def worker(bs):
    global client
    client.deferred.addCallback(worker)

    bs.watch("myqueue")
    bs.ignore("default")

    coop = task.Cooperator()
    coop.coiterate(executionGenerator(bs))

client = beanstalk.twisted_client.BeanstalkClient(noisy=True)
d = client.connectTCP(sys.argv[1], 11300)
d.addCallback(worker)
reactor.run()
