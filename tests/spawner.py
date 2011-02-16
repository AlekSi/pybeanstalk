import time
from subprocess import Popen

class BeanstalkdSpawner(object):
    """
    Spawner of beanstalkd instances for tests.
    """

    instances = []

    def spawn(self, host='0.0.0.0', port=11300, path='beanstalkd'):
        """
        Spawns new instance.
        """

        p = Popen([path, "-l", host, "-p", port])
        self.instances.append(p)
        time.sleep(0.1)

    def terminate_all(self):
        """
        Terminates all instances.
        """

        for instance in self.instances:
            instance.terminate()
            instance.wait()

        self.instances = []


spawner = BeanstalkdSpawner()
