from twisted.internet import defer


class WaitingDeferred(object):
    """
    Anti-DeferredList.

    We have some resource, and several actors waiting
    on it. Every actor should get notified via Deferred
    about resource availability.

    @ivar queue: queue of waiting Deferreds (actors)
    @type queue: C{list}
    @ivar fired: was this Deferred fired?
    @type fired: C{bool}
    """

    def __init__(self):
        """
        Constructor.
        """
        self.queue = []
        self.fired = False

    def push(self):
        """
        One more actor wants to get the resource.

        Give him Deferred!

        @return: Deferred, resulting in resource
        @rtype: C{Deferred}
        """
        assert not self.fired

        d = defer.Deferred()
        self.queue.append(d)
        return d

    def callback(self, *args, **kwargs):
        """
        We got resource, pass it to all waiting actors.
        """
        assert not self.fired

        self.fired = True

        for d in self.queue:
            d.callback(*args, **kwargs)

    def errback(self, *args, **kwargs):
        """
        We got error, propagate it to actors.
        """
        assert not self.fired

        self.fired = True

        for d in self.queue:
            d.errback(*args, **kwargs)
