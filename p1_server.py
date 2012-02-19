#!/usr/bin/env python

import re, optparse, time, os, sys, threading, heapq, signal, binascii
import zmq
from p1_common import *
from datetime import datetime, timedelta

def miliseconds_from(timestamp):
    (datetime.now() - timestamp).microseconds / 1000

def is_ping(msg):
    prev_empty = False
    for part in msg:
        if len(part) == 0:
            prev_empty = True
            continue
        if part == "ping":
            return True
        prev_empty = False
    return False

class Task:
    (WAITING, PAIRED, COMPLETED) = (1, 2, 3)

    def __init__(self):
        self.clear()
    def clear(self):
        self.request_id = None
        self.client_id = None
        self.worker_id = None
        self.client_hb = None
        self.worker_hb = None
        self.status = Task.WAITING
        self.payload = []

context = zmq.Context()

class Broker(threading.Thread):
    tasks = []
    def __init__(self, front_url, back_url, stale_client_age=3, stale_worker_age=3):
        super(Broker, self).__init__()
        self.front_socket = context.socket(zmq.ROUTER)     # Client side socket
        self.front_socket.bind(front_url)
        self.back_socket = context.socket(zmq.ROUTER)   # Worker side socket
        self.back_socket.bind(back_url)
        self.stale_client_age = stale_client_age              # After how many seconds the client is considered stale
        self.stale_worker_age = stale_worker_age              # After how many seconds the worker is considered stale
        self.sent_client_hb_at = None                         # Last time we sent a heartbeat to the clients
        self.send_client_hb_interval = 1000                   # How often to send heartbeats to the clients (miliseconds)

    def stale_client(self, task):
        return task.client_hb and miliseconds_from(task.client_hb) > self.stale_client_age

    def stale_worker(self, task):
        return task.worker_hb and miliseconds_from(task.worker_hb) > self.stale_worker_age

    def make_task_stale(self, task):
        task.clear()

    def notify_all(self):
        if self.sent_client_hb_at and miliseconds_from(self.sent_client_hb_at) < self.send_client_hb_interval:
            return # No need to notify
        for task in self.tasks:
            if task.client_id:
                # client_id is used for routing
                msg = [task.client_id, "", HEARTBEAT, task.request_id]
                self.front_socket.send_multipart(msg)

    def pair_client_and_worker(self, task):
        if task.client_id and task.worker_id and task.status == Task.WAITING:
            task.status = Task.PAIRED
            print "-------------- PAIR -----------------"
            # worker_id is used for routing
            msg = [task.worker_id, "", TASK_AVAILABLE, task.request_id] + task.payload
            self.back_socket.send_multipart(msg)

    def received_task_available(self, client_id, request_id, payload):
        print "***** task available"
        found_task = None
        for task in self.tasks:
            if task.worker_id and not task.client_id:
                found_task = task
                break
        if found_task:
            task = found_task
        else:
            task = Task()
            self.tasks.append(task)

        task.client_id = client_id
        task.request_id = request_id
        task.payload = payload
        task.client_hb = datetime.now()

        self.pair_client_and_worker(task)

    def received_worker_available(self, worker_id):
        for task in self.tasks:
            if task.worker_id == worker_id:
                print "+++++++ Worker known"
                # Do nothing, we already know about this worker. TODO: maybe cancel the current task for this worker
                return
        found_task = None
        for task in self.tasks:
            if task.client_id and not task.worker_id:
                found_task = task

        if not found_task:
            task = Task()
            self.tasks.append(task)
        else:
            task = found_task
        task.worker_id = worker_id
        task.worker_hb = datetime.now()
        self.pair_client_and_worker(task)

    def received_work_result(self, worker_id, request_id, payload):
        for task in self.tasks:
            if task.worker_id == worker_id and task.request_id == request_id:
                # client_id is used for routing
                msg = [task.client_id, "", TASK_COMPLETE, task.request_id] + payload
                self.front_socket.send_multipart(msg)
                self.make_task_stale(task) # for purge
                self.purge()

    def received_client_hb(self, client_id):
        for task in self.tasks:
            if task.client_id == client_id:
                task.client_hb = datetime.now()

    def received_worker_hb(self, worker_id):
        for task in tasks:
            if task.worker_id == worker_id:
                task.worker_hb = datetime.now()

    def purge(self):
        purged_tasks = []
        for task in self.tasks:
            stale = not (task.client_id or task.worker_id) or self.stale_client(task) or self.stale_worker(task)
            if not stale:
                purged_tasks.append(task)
        self.tasks = purged_tasks
    
    def run(self):
        available_workers = set()
        tasks = []
        while True:
            cleanup_timestamp = datetime.now()
            poll = zmq.Poller()
            poll.register(self.front_socket, zmq.POLLIN)
            poll.register(self.back_socket, zmq.POLLIN)
            sockets = dict(poll.poll(1000))
            if self.front_socket in sockets and sockets[self.front_socket] == zmq.POLLIN:
                msg = self.front_socket.recv_multipart()
                msg_type = parse_message_type(msg)
                print "--> Broker FE %s - %s" % (msg_type, dump_message(msg))
                if msg_type == TASK_AVAILABLE:
                    (client_id, msg_type, request_id, payload) = parse_message(msg, 4)
                    self.received_task_available(client_id, request_id, payload)
                elif msg_type == HEARTBEAT:
                    (client_id, msg_type) = parse_message(msg, 2)
                    self.received_client_hb(client_id)
                elif msg_type == TASK_CANCEL:
                    pass
                else:
                    print "FS: Unknown message received"
            if self.back_socket in sockets and sockets[self.back_socket] == zmq.POLLIN:
                msg = self.back_socket.recv_multipart()
                print "<-- Broker BE %s" % dump_message(msg);
                msg_type = parse_message_type(msg)
                if msg_type == WORKER_AVAILABLE:
                    (worker_id, msg_type) = parse_message(msg, 2)
                    self.received_worker_available(worker_id)
                elif msg_type == HEARTBEAT:
                    (worker_id, msg_type) = parse_message(msg, 2)
                    self.received_worker_available(worker_id)
                elif msg_type == TASK_COMPLETE:
                    (worker_id, msg_type, request_id, payload) = parse_message(msg, 4)
                    self.received_work_result(worker_id, request_id, payload)
                else:
                    print "BS: Unknown message received"
            if (datetime.now() - cleanup_timestamp).seconds >= 1:
                self.purge()
                self.notify_all()
                cleanup_timestamp = datetime.now()

class LoadBalancer(threading.Thread):
    available = set()
    def __init__(self, front_url, back_url):
        super(LoadBalancer, self).__init__()
        self.front_socket = context.socket(zmq.DEALER)     # Client side socket
        self.front_socket.connect(front_url)
        self.back_socket = context.socket(zmq.ROUTER)      # Worker side socket
        self.back_socket.bind(back_url)

    def run(self):
        while True:
            poll = zmq.Poller()
            if len(self.available) > 0:
                poll.register(self.front_socket, zmq.POLLIN)
            poll.register(self.back_socket, zmq.POLLIN)
            sockets = dict(poll.poll(1000))
            if self.front_socket in sockets and sockets[self.front_socket] == zmq.POLLIN:
                msg = self.front_socket.recv_multipart()
                msg_type = parse_message_type(msg)
                print "<-- LB FS: Received %s" % dump_message(msg);
                if msg_type == TASK_AVAILABLE:
                    (_, message_type, task_id, payload) = parse_message(msg, 4)
                    worker_id = self.available.pop()
                    self.back_socket.send_multipart([worker_id, "", TASK_AVAILABLE, task_id] + payload)
            if self.back_socket in sockets and sockets[self.back_socket] == zmq.POLLIN:
                msg = self.back_socket.recv_multipart()
                msg_type = parse_message_type(msg)
                print "<-- LB BS: Received %s" % dump_message(msg);
                if msg_type == WORKER_AVAILABLE:
                    worker_id = msg[0]
                    self.available.add(worker_id)
                    self.front_socket.send_multipart(["", WORKER_AVAILABLE])
                elif msg_type == TASK_COMPLETE:
                    (worker_id, message_type, task_id, payload) = parse_message(msg, 4)
                    self.front_socket.send_multipart(["", TASK_COMPLETE, task_id] + payload)
                else:
                    print "LB BS: Unknown message received"


# class Router:
#     def __init__(self, front_end_url, back_end_url):
#         self.front_socket = context.socket(zmq.ROUTER)
#         self.back_socket = context.socket(zmq.ROUTER)
#         self.front_socket.bind(front_end_url)
#         self.back_socket.bind(back_end_url)
#     
#     def run(self):
#         available_workers = set()
#         while True:
#             poll = zmq.Poller()
#             if len(available_workers) > 0:
#                 poll.register(self.front_socket, zmq.POLLIN)
#             poll.register(self.back_socket, zmq.POLLIN)
#             sockets = dict(poll.poll(3000))
#             if self.front_socket in sockets and sockets[self.front_socket] == zmq.POLLIN:
#                 msg = self.front_socket.recv_multipart()
#                 print "--> front end %s" % dump_message(msg)
#                 if is_ping(msg):
#                     self.front_socket.send_multipart(msg)
#                 else:
#                     peer_id = available_workers.pop()
#                     print "Sending to peer %s" % binascii.hexlify(peer_id)
#                     #self.back_socket.send_multipart(msg)
#                     msg = [peer_id] + msg
#                     self.back_socket.send_multipart(msg)
#             if self.back_socket in sockets and sockets[self.back_socket] == zmq.POLLIN:
#                 msg = self.back_socket.recv_multipart()
#                 print "<-- back end %s" % dump_message(msg);
#                 peer_ident = msg[0]
#                 available_workers.add(peer_ident)
# 
#                 # Remove peer from the address chain and put it at the bottom of the message
#                 msg.append(msg.pop(0))
# 
#                 # We should not send "hello" messages back to the client, but discard them
#                 self.front_socket.send_multipart(msg)

class Worker(threading.Thread):
    def __init__(self, router_url):
        super(Worker, self).__init__()
        self.socket = context.socket(zmq.DEALER)
        self.socket.connect(router_url)

    def run(self):
        while True:
            self.socket.send_multipart(["", WORKER_AVAILABLE])
            print "Worker available %s" % threading.current_thread()

            poll = zmq.Poller()
            poll.register(self.socket, zmq.POLLIN)
            sockets = dict(poll.poll(1000))
            if self.socket in sockets and sockets[self.socket] == zmq.POLLIN:
                msg = self.socket.recv_multipart()
                message_type = parse_message_type(msg)
                print "--> W: Received %s" % dump_message(msg);
                if message_type == TASK_AVAILABLE:
                    (_, message_type, task_id, payload) = parse_message(msg, 4)
                    self.do_work()
                    self.socket.send_multipart(["", TASK_COMPLETE, task_id, "Computation done on " + str(datetime.now())])
                else:
                    print "Worker received an unknown message"

    def do_work(self):
        print "*** Work done %s" % threading.current_thread()
        #time.sleep(0)
        pass


def handler(signum, frame):
    sys.stderr.write("\nExiting...\n")
    os._exit(1)

def main():

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    try:
        broker_front_url = "tcp://127.0.0.1:5666"
#        broker_back_url = "tcp://127.0.0.1:5777"
#        load_balancer_url = "tcp://127.0.0.1:5888"
        broker_back_url = "inproc://broker"
        load_balancer_url = "inproc://load_balancer"

        broker = Broker(broker_front_url, broker_back_url)
        broker.start()
        time.sleep(0.1)

        load_balancer = LoadBalancer(broker_back_url, load_balancer_url)
        load_balancer.start()
        time.sleep(0.1)

        for i in range(0, 4):
            worker = Worker(load_balancer_url)
            worker.start()

        while True:
            time.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        sys.stderr.write("Exiting...\n")
        abort_early = True
        os._exit(1)

    os._exit(2)

main()




