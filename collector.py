import random
import time
import threading
import zmq
import logging
from logging import debug, info
from itertools import count

READY = "READY"
context = zmq.Context()
counter = count()

def checkzmqerror(func):
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except zmq.ZMQError, err:
            info("%r(*%r, **%r) is terminating with error %s", func, args, kwargs, err)
    return wrapper

def collector(name, frontend, backend, timeout):
    backends = set()
    debug("collector %s is ready with %r backends", name, len(backends))
    dropped = 0
    while True:
        poller = zmq.Poller()
        poller.register(backend, zmq.POLLIN)
        poller.register(frontend, zmq.POLLIN)
        for socket, event in poller.poll():
            request = socket.recv_multipart()
            debug("collector %s received request %r", name, request)
            if socket is backend:
                if request[2] == READY:
                    debug("collector %s has new backend: %r", name, request[0])
                    backends.add(request[0])
                else:
                    debug("collector %s discard reply %r", name, request) 
            else:
                delim = request.index("")
                address_stack = request[:delim+1]
                debug("collector %s has new work to do", name)
                recipients = backends
                backends = set()
                debug("collector %s send requests to %r", name, recipients)
                for dest in recipients:
                    backend.send_multipart([dest] + request[delim:])
                poller = zmq.Poller()
                poller.register(backend, zmq.POLLIN)
                start = time.time()
                deadline = start + timeout / 1000.0
                while recipients:
                    debug("%r: collector %s wait on on %r", start, name, recipients)
                    events = poller.poll(max(0,deadline-time.time()))
                    for socket, event in events:
                        reply = socket.recv_multipart()
                        if reply[2] == READY:
                            debug("%r is ready on %s", reply[0], name)
                            backends.add(reply[0])
                            recipients.discard(reply[0])
                        elif reply[0] in recipients:
                            debug("collector %s forward reply", name)
                            frontend.send_multipart(address_stack + reply[2:])
                        else:
                            debug("collector %s discard reply %r", name, reply)
                    end = time.time()
                    if recipients and end > deadline:
                        info("%r: collector %s has timeout with %d recipients", end, name, len(recipients))
                        break
                frontend.send_multipart(address_stack + [READY])
                debug("collector %s is ready with %r backends", name, len(backends))


@checkzmqerror
def broker_collector(frontend_url, backend_url, timeout):
    frontend = context.socket(zmq.XREP)
    frontend.setsockopt(zmq.IDENTITY, backend_url)
    backend = context.socket(zmq.XREP)
    info("Binding broker frontend to %s", frontend_url)
    frontend.bind(frontend_url)
    info("Binding broker backend to %s", backend_url)
    backend.bind(backend_url)
    collector("broker", frontend, backend, timeout)

@checkzmqerror
def proxy_collector(frontend_url, backend_url, timeout):
    frontend = context.socket(zmq.XREQ)
    frontend.setsockopt(zmq.IDENTITY, backend_url)
    backend = context.socket(zmq.XREP)
    info("Connecting proxy frontend to %s", frontend_url)
    frontend.connect(frontend_url)
    info("Binding proxy backend to %s", backend_url)
    # Sending presence to frontend.
    backend.bind(backend_url)
    frontend.send_multipart(["", READY])
    collector("proxy", frontend, backend, timeout)

def worker(socket, workload, failure_rate = 0):
    while True:
        debug("Worker is ready")
        socket.send_multipart(["",READY])
        request = socket.recv_multipart()
        debug("Worker receive request %r", request)
        content = request.index("") + 1
        address = request[:content]
        request = request[content:]
        assert request[0] == "REQUEST"
        if failure_rate and random.randrange(failure_rate) == 0:
            info("worker failed")
            return False
        time.sleep(workload * (1 + random.random()))
        debug("worker send reply")
        socket.send_multipart(address + [request[1], "DONE"])

@checkzmqerror
def connect_worker(url, workload, failure_rate = 0):
    while True:
        socket = context.socket(zmq.XREQ)
        info("Connecting worker to %s", url)
        socket.connect(url)
        worker(socket, workload, failure_rate)

def requester(socket):
    while True:
        i = str(counter.next())
        info("Requester send request %s", i)
        socket.send_multipart(["", "REQUEST", i])
        results = 0
        while True:
            reply = socket.recv_multipart()
            debug("requester received reply %r", reply)
            if reply == ["",READY]:
                break
            assert reply[1] == i
            results += 1
        info("requester received %d results", results)

@checkzmqerror
def connect_requester(url):
    socket = context.socket(zmq.XREQ)
    info("Connecting requester to %s", url)
    socket.connect(url)
    requester(socket)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    feurl = "inproc://frontend"
    beurl = "inproc://backend"
    workload = 2.5
    broker_timeout = 5000
    proxy_timeout = 5000
    brokers = []
    broker = threading.Thread(target = broker_collector, args = (feurl, beurl, broker_timeout))
    broker.start()
    brokers.append(broker)
    time.sleep(2)
    senders = []
    for sender in xrange(10):
        sender = threading.Thread(target = connect_requester, args = (feurl,))
        sender.start()
        senders.append(sender)
    proxies = []
    proxy_urls = []
    for proxy in xrange(5):
        url = "inproc://proxy_be#%d" % (proxy,)
        proxy = threading.Thread(target = proxy_collector, args = (beurl, url, proxy_timeout))
        proxy.start()
        proxies.append(proxy)
        proxy_urls.append(url)
    time.sleep(2)
    workers = []
    for url in proxy_urls:
        for work in xrange(5):
            work = threading.Thread(target = connect_worker, args = (url, 3, 10))
            work.start()
            workers.append(work)
    time.sleep(20)
    info("Joining thread")
    context.term()
    for thread in senders + brokers + proxies + workers:
        thread.join()

