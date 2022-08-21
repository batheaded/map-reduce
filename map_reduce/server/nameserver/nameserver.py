import logging, time
from typing import Union
from threading import Thread

import Pyro4
import Pyro4.errors
import Pyro4.naming
from Pyro4 import URI, Proxy
from Pyro4.naming import NameServerDaemon, BroadcastServer
from map_reduce.server.configs import NS_CONTEST_INTERVAL

from map_reduce.server.utils import alive, reachable, id, kill_thread, spawn_thread
from map_reduce.server.logger import get_logger


logger = get_logger('ns')

class NameServer:
    '''
    A wrapper around a Pyro nameserver connection.

    Searches for a nameserver to bind to in the local network, otherwise starts up
    a nameserver thread from this machine. Multiple, simultaneous nameservers in the
    network are contested by hash/id precedence.

    TODO:
    [ ] Nameserver persistance (perhaps on DHT?).
    [ ] Online/offline API as an external delegation alternative.
        [ ] Perhaps this entails a serious feature extrapolation from this class to another.
    [ ] After previous item:
        [ ] Implement master nodes on top of nameserver.
    '''
    def __init__(self, ip, port=8008):
        ''' Instantiates a new nameserver, then starts it up. '''
        # Self attributes.
        self._ip = ip
        self._port = port
        self._alive = False
        self._uri: URI = None
        self._ns_daemon: NameServerDaemon = None
        self._ns_broadcast: BroadcastServer = None
        self._ns_thread: Thread = None
        self._broadcast_thread: Thread = None
        self._stabilization_thread: Thread = None

        # Logger config.
        global logger
        logger = logging.LoggerAdapter(logger, {'IP': ip})
        
    def __str__(self):
        status = 'remote' if self.is_remote else 'local'
        return f'{self.__class__.__name__}({status})@[{self._uri}]'
    
    def __repr__(self):
        return str(self)
    
    @property
    def is_remote(self) -> bool:
        return self._ip != self._uri.host
    
    @property
    def is_local(self) -> bool:
        return not self.is_remote
    
    @property
    def servers(self) -> tuple[Pyro4.Daemon, Pyro4.naming.BroadcastServer]:
        return self._ns_daemon, self._ns_broadcast
    
    def _locate_nameserver(self) -> Union[URI, None]:
        '''
        Attempts to locate a remote nameserver. Returns its URI if found.
        '''
        try:
            with Pyro4.locateNS() as ns:
                return ns._pyroUri
        except Pyro4.errors.NamingError:
            return None
    
    def _start_local_nameserver(self):
        '''
        Starts the local nameserver on a parallel thread.
        '''
        logger.info(f'Local nameserver started.')
        self._uri, self._ns_daemon, self._ns_broadcast = Pyro4.naming.startNS(self._ip,
                                                                              self._port)
        self._ns_thread = spawn_thread(target=self._ns_daemon.requestLoop)
        self._broadcast_thread = spawn_thread(target=self._ns_broadcast.runInThread)
    
    def _stop_local_nameserver(self, forward_to: URI = None):
        '''
        Stops the local nameserver (killing its thread and daemons).
        '''
        if forward_to is not None:
            # Forward registered objects to new nameserver.
            new_ns = forward_to
            try:
                with Proxy(self._uri) as sender, Proxy(new_ns) as recv:
                    sender: Pyro4.naming.NameServer
                    for name, addr in sender.list().items():
                        try:
                            recv.register(name, addr, safe=True)
                        except Pyro4.errors.NamingError as e:
                            logger.info(f'{e}')
            except Exception as e:
                logger.error(f"Error forwarding registry to nameserver {new_ns.host!r}: {e}")
            
            # Overwrite the binding address.
            self._uri = new_ns

        # Shutdown the nameserver.
        self._ns_daemon.shutdown()
        self._ns_daemon = None
        kill_thread(self._ns_thread, logger)
        
        # Shutdown the broadcast utility server.
        self._ns_broadcast.close()
        self._ns_broadcast = None
        kill_thread(self._broadcast_thread, logger)
        
        logger.info(f'Local nameserver stopped.')
    
    def refresh_nameserver(self):
        '''
        Stabilizes the nameserver reference, either when a remote nameserver dies, or
        when a remote nameserver appears that should be leader instead of the local one.
        Note that this can kill the nameserver daemon and its broadcast server so
        it should be used carefully.

        This method is called periodically by the start() method, but can be used
        externally when sequential checks are needed instead of a parallel thread.
        '''
        curr_ns = self._uri
        new_ns = self._locate_nameserver()

        if self.is_remote:
            if not reachable(curr_ns):
                logger.warning(f'Remote nameserver @{curr_ns.host} is not reachable.')
                if new_ns is not None:
                    logger.info(f'Found new nameserver @{new_ns.host}.')
                    self._uri = new_ns
                else:
                    logger.info(f'No new nameserver found. Announcing self.')
                    self._start_local_nameserver()
        else:
            if new_ns is not None and new_ns != curr_ns:
                logger.info(f'Found contesting nameserver @{new_ns.host}.')
                if id(curr_ns) >= id(new_ns):
                    logger.info(f'I no longer am the nameserver, long live {new_ns.host}.')
                    self._stop_local_nameserver(forward_to=new_ns)
                else:
                    logger.info(f'I am still the nameserver.')

    def start(self):
        '''
        Starts the nameserver wrapper. Spawns a checker thread that assures only one
        nameserver is alive in the network at any given moment.
        '''
        self._start_local_nameserver()

        def nameserver_loop():
            self._alive = True
            while self._alive:
                self.refresh_nameserver()
                time.sleep(NS_CONTEST_INTERVAL)

        logger.info('Nameserver checker loop starting...')
        self._stabilization_thread = spawn_thread(target=nameserver_loop)
    
    def stop(self):
        '''
        Stops the nameserver wrapper and the created threads.
        '''
        self._alive = False
        kill_thread(self._stabilization_thread, logger)
        if self.is_local:
            self._stop_local_nameserver()

    def bind(self) -> Pyro4.naming.NameServer:
        '''
        Returns a proxy bound to the nameserver, local or remote. Should be used
        with a context manager.
        '''
        assert self._alive, 'NameServer instance must be started before binding.'
        return Proxy(self._uri)