from twisted.protocols import basic
from twisted.internet import reactor, defer, protocol
from twisted.python import log, failure
import protohandler

# Stolen from memcached protocol
try:
    from collections import deque
except ImportError:
    class deque(list):
        def popleft(self):
            return self.pop(0)
        def appendleft(self, item):
            self.insert(0, item)

class Command(object):
    """
    Wrap a client action into an object, that holds the values used in the
    protocol.

    @ivar _deferred: the L{Deferred} object that will be fired when the result
        arrives.
    @type _deferred: L{Deferred}

    @ivar command: name of the command sent to the server.
    @type command: C{str}
    """

    def __init__(self, command, handler, **kwargs):
        """
        Create a command.

        @param command: the name of the command.
        @type command: C{str}

        @param kwargs: this values will be stored as attributes of the object
            for future use
        """
        self.command = command
        self.handler = handler
        self._deferred = defer.Deferred()
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return "<Command: %s>" % self.command

    def success(self, value):
        """
        Shortcut method to fire the underlying deferred.
        """
        self._deferred.callback(value)


    def fail(self, error):
        """
        Make the underlying deferred fails.
        """
        self._deferred.errback(error)


class Beanstalk(basic.LineReceiver):
    def __init__(self):
        self._current = deque()

    def connectionMade(self):
        self.setLineMode()

    def _cmd(self, command, full_command, handler):
        # Note here: the protohandler already inserts the \r\n, so
        # it would be an error to do self.sendline()
        self.transport.write(full_command)
        cmdObj = Command(command, handler)
        self._current.append(cmdObj)
        return cmdObj._deferred

    def lineReceived(self, line):
        """
        Receive line commands from the server.
        """
        pending = self._current.popleft()
        try:
            # this is a bit silly as twisted is so nice as to remove the
            # eol from the line, but protohandler.Handler needs it...
            # the reason its needed is that protohandler needs to work
            # in situations without twisted where things aren't so nice
            res = pending.handler(line + "\r\n")
        except Exception, e:
            pending.fail(e)
        else:
            if res is not None: # we have a result!
                pending.success(res)
            else: # there is more data, its a job or something...
                # push the pending command back on the stack
                self._current.appendleft(pending)
                self.setRawMode()

    def rawDataReceived(self, data):
        pending = self._current.popleft()
        if len(data) >= pending.handler.remaining:
            rem = data[pending.handler.remaining:]
            data = data[:pending.handler.remaining]
        else:
            rem = None

        try:
            res = pending.handler(data)
        except Exception, e:
            pending.fail(e)
            self.setLineMode(rem)
        if res:
            pending.success(res)
            self.setLineMode(rem)
        else:
            self._current.appendleft(pending)

for name in dir(protohandler):
    if not name.startswith('process_'):
        continue
    cmd = name.partition('_')[2]

    def wrapper(cmd):
        def caller(self, *args, **kw):
            return self._cmd(cmd,
                *getattr(protohandler, 'process_%s' % cmd)(*args, **kw))
        return caller

    setattr(Beanstalk, cmd, wrapper(cmd))


class BeanstalkClientFactory(protocol.ReconnectingClientFactory):
    """
    Handles disconnects.
    """

    noisy = False
    protocol = Beanstalk

    client = None

    def __init__(self, _client=None):
        self.client = _client

    def buildProtocol(self, addr):
        if self.noisy:
            log.msg("BeanstalkClientFactory - buildProtocol %r" % addr)
        self.resetDelay()
        proto = protocol.ReconnectingClientFactory.buildProtocol(self, addr)
        self._fire_later('callback', proto)
        return proto

    def clientConnectionFailed(self, connector, reason):
        if self.noisy:
            log.msg("BeanstalkClientFactory - clientConnectionFailed: %s" % reason)
        self._fire_later('callback', None)
        return protocol.ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        if self.noisy:
            log.msg("BeanstalkClientFactory - clientConnectionLost: %s" % reason)
        self._fire_later('callback', None)
        return protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def _fire_later(self, method, proto):
        if self.client:
            if self.noisy:
                log.msg("BeanstalkClientFactory - _fire_later %s %r" % (method, proto))
            reactor.callLater(0, self.client._fire, method, (self.client, proto))


class BeanstalkClient(object):
    """
    @ivar deferred: callbacks (client, proto), where client is this L{BeanstalkClient} instance and
                    proto is L{Beanstalk} on connect and C{None} on disconnect.
    @type deferred: L{Deferred}
    """

    noisy = False
    deferred = None
    protocol = None

    def __init__(self, noisy=False):
        self.noisy = noisy
        self._fire('', (self, None))

    def _fire(self, method, (client, proto)):
        if self.noisy:
            log.msg("BeanstalkClient - _fire %s %r" % (method, (client, proto)))
        assert self == client
        self.protocol = proto
        m = getattr(self.deferred, method, None)
        self.deferred = defer.Deferred()
        if m:
            return m((client, proto))

    def connectTCP(self, host, port):
        f = BeanstalkClientFactory(self)
        f.noisy = self.noisy
        reactor.connectTCP(host, port, f)
        return self.deferred

    def disconnect(self):
        if self.protocol:
            self.protocol.factory.stopTrying()
            self.protocol.transport.loseConnection()
        return self.deferred
