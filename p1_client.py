#!/usr/bin/env python

import re, optparse, time, os, sys, threading, heapq, random, signal
import zmq
from p1_common import *
from datetime import datetime

class Client:
    def random_id(self):
        chars="abcdefgABCDEFT1234567890"
        return ''.join(map(lambda x: random.choice(chars), range(0, 15)))

    def __init__(self, context, url):
        self.context = context
        self.url = url
        self.socket = None
        self.reconnect()

# def receive_within(socket, timeout, retries):
#     poll = zmq.Poller()
#     poll.register(socket, zmq.POLLIN)
# 
#     for i in range(0, 5):
#         sockets = dict(poll.poll(1000))
#         if socket in sockets:
#             if sockets[socket] == zmq.POLLIN:
#                 msg = socket.recv_multipart()
#                 return msg
#         print "Timeout. Retry: %d" % i
#     return None

    def reconnect(self):
        sys.stderr.write("Connecting\n")
        if self.socket:
            self.socket.close()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(self.url)

    def send_message(self, msg, timeout = 10, retries = 3, hb_timeout=2, request_id = None):
        start_time = datetime.now()
        prev_hb = datetime.now()
        broker_hb = datetime.now()
        retries = 5
        if not request_id:
            request_id = self.random_id()
        print "rid=%s" % request_id
        self.socket.send_multipart(["", CL_TASK_AVAILABLE, request_id] + msg)
        while True:
            poll = zmq.Poller()
            poll.register(self.socket, zmq.POLLIN)
            sockets = dict(poll.poll(1000))
            if self.socket in sockets and sockets[self.socket] == zmq.POLLIN:
                msg = self.socket.recv_multipart()
                print "<-- %s" % dump_message(msg)
                message_type = parse_message_type(msg)
                if message_type == BR_TASK_COMPLETE:
                    (_, message_type, recv_request_id, payload) = parse_message(msg, 4)
                    if request_id == recv_request_id:
                        return (True, payload)
                    else:
                        print "Invalid task ID received"
                elif message_type == BR_HEARTBEAT:
                    (_, message_type, recv_request_id) = parse_message(msg, 2)
                    if (recv_request_id == request_id):
                        broker_hb = datetime.now()
                    else:
                        sys.stderr.write("Invalid broker heartbeat, bad request ID")
                    
            if (datetime.now() - start_time).seconds > timeout:
                break
            if (datetime.now() - prev_hb).seconds > 1:
                print "Sending heartbeat"
                self.socket.send_multipart(["", CLIENT_HB, request_id]) 
                prev_hb = datetime.now()
            if (datetime.now() - broker_hb).seconds > hb_timeout:
                system.out.write("Broker died (no heartbeat): retry %d" % retry)
        sys.stderr.write("Request %s timed out\n" % request_id)
        self.reconnect()
        return (False, None)

def test():  
    url = "tcp://localhost:5666"
    context = zmq.Context()
    client = Client(context, url)

    requests = 10000
    start_time = time.time()
    for i in range(0, requests):
        (success, response) = client.send_message(["some work from you"], request_id=str(i))
        if success:
            print "Response: %s" % dump_message(response)
        if i % 1000 == 0:
            sys.stderr.write("Requests: %d\n" % i)

    req_s = float(requests) / (time.time() - start_time)

    sys.stderr.write("%d req/s\n" % req_s)

def catch_signals():
    def handler(signum, frame):
        sys.stderr.write("\nExiting...\n")
        sys.exit(1)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

class Runner(threading.Thread):
    def __init__(self):
        super(Runner, self).__init__()

    def run(self):
        test()

if __name__ == "__main__":
    catch_signals()
    # for i in range(0, 4):
    #      runner = Runner()
    #      runner.start()
    test()
