from multiprocessing.managers import BaseManager
from multiprocessing import Queue, Manager
from typing import List, Tuple


class HeldHost:
    def __init__(self, item, queue):
        self.ansible_env = item[0]
        self.address = item[1]
        self.queue = queue

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.queue.put((self.ansible_env, self.address))

    def __str__(self):
        return f"{self.ansible_env} - {self.address}"


class CustManager(BaseManager):
    pass


class CustQueue:
    """
    A multiprocessing queue for distributing access to shared hosts during an experiment run.
    """

    def __init__(self, items: List[Tuple[str, str]], queue: Queue):
        """
        Takes a list of environment names and host addresses to insert into new queue.
        """
        for item in items:
            queue.put(item)
        self.queue = queue

    def get(self):
        """
        Items from this should be passed to a new HeldHost and used in a `with` statement.
        ```
        with HeldHost(queue.get(), queue) as held_host:
        ```
        """
        item = self.queue.get()
        return item
        # Python bug? process hangs if trying to return this
        # Works with minimal example though...
        return HeldHost(item, self.queue)

    def put(self, host):
        self.queue.put(host)


CustManager.register("CustQueue", CustQueue)
CustManager.register("Queue", Queue)
m2 = Manager()
m = CustManager()
m.start()


def make_host_queue(host_names: List[Tuple[str, str]]):
    """
    Create a new shared host queue with the given host information.
    Pairs of (<Ansible host environment name>, <hostname/IP address>)
    """
    return m.CustQueue(host_names, m2.Queue())


LOCALHOST_Q = make_host_queue([("local", "127.0.0.1")])
