#!/usr/bin/env python

import re, optparse, time, os, sys, threading, heapq, random, signal
import zmq
from p1_common import *
from datetime import datetime

def random_id():
    chars="abcdefgABCDEFT1234567890"
    return ''.join(map(lambda x: random.choice(chars), range(0, 15)))

def receive_within(socket, timeout, retries):
    poll = zmq.Poller()
    poll.register(socket, zmq.POLLIN)

    for i in range(0, 5):
        sockets = dict(poll.poll(1000))
        if socket in sockets:
            if sockets[socket] == zmq.POLLIN:
                msg = socket.recv_multipart()
                return msg
        print "Timeout. Retry: %d" % i
    return None

def send_message(socket, msg, request_id = None):
    start_time = datetime.now()
    prev_hb = datetime.now()
    retries = 5
    if not request_id:
        request_id = random_id()
    print "rid=%s" % request_id
    socket.send_multipart(["", CL_TASK_AVAILABLE, request_id] + msg)
    while True:
        poll = zmq.Poller()
        poll.register(socket, zmq.POLLIN)
        sockets = dict(poll.poll(1000))
        if socket in sockets and sockets[socket] == zmq.POLLIN:
            msg = socket.recv_multipart()
            print "<-- %s" % dump_message(msg)
            message_type = parse_message_type(msg)
            if message_type == BR_TASK_COMPLETE:
                (_, message_type, recv_request_id, payload) = parse_message(msg, 4)
                if request_id == recv_request_id:
                    return payload
                else:
                    print "Invalid task ID received"
            elif message_type == BR_HEARTBEAT:
                print "BR_HEARTBEAT"
            else:
                print "Invalid message type received %s" % message_type
        if (datetime.now() - start_time).seconds > 5:
            break
        if (datetime.now() - prev_hb).seconds > 1:
            print "Sending heartbeat"
            socket.send_multipart(["", CLIENT_HB, request_id]) 
            prev_hb = datetime.now()
        sys.stdout.write(".")
    return []


def test():  
    url = "tcp://localhost:5666"
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.connect(url)

    requests = 1000000 / 3;
    for i in range(0, requests):
        response = send_message(socket, ["some work from you"], str(i))
        print "Response: %s" % dump_message(response)


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
