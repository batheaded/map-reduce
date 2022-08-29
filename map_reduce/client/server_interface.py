import Pyro4
import Pyro4.socketutil
import threading
from typing import Any

from map_reduce.server.utils import spawn_thread

AWAIT_INTERVAL = 1
IP = Pyro4.socketutil.getIpAddress(None, workaround127=None)

class ServerInterface:
    results = None
    results_lock = threading.Lock()

    @Pyro4.expose
    @classmethod
    def notify_results(cls, results: Any):
        cls.results = results
        cls.results_lock.release()
    
    @classmethod
    def startup(cls, data, map_f, reduce_f) -> Pyro4.Daemon:
        # Acquire lock for await function.
        cls.results_lock.acquire()

        # Instance a daemon to expose class.
        daemon = Pyro4.Daemon(host=IP, port=8008)
        addr = daemon.register(cls, 'client')

        # Start the request for map-reduce server.
        with Pyro4.locateNS as ns:
            with ns.lookup('request-handler') as server:
                if server.startup(addr, data, map_f, reduce_f):
                    spawn_thread(target=daemon.requestLoop)
                    return daemon
                else:
                    print("Server couldn't start up due to a communication error.")
                    return None
    
    @classmethod
    def await_results(cls):
        cls.results_lock.acquire()
        cls.results_lock.release()