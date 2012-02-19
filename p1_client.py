#!/usr/bin/env python

import re, optparse, time, os, sys, threading, heapq, random
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

def send_message(socket, msg):
    start_time = datetime.now()
    prev_hb = datetime.now()
    retries = 5
    task_id = random_id()
    socket.send_multipart(["", TASK_AVAILABLE, task_id] + msg)
    while True:
        poll = zmq.Poller()
        poll.register(socket, zmq.POLLIN)
        sockets = dict(poll.poll(1000))
        if socket in sockets and sockets[socket] == zmq.POLLIN:
            msg = socket.recv_multipart()
            print "<-- %s" % dump_message(msg)
            message_type = parse_message_type(msg)
            if message_type == TASK_COMPLETE:
                (_, message_type, recv_task_id, payload) = parse_message(msg, 4)
                if task_id == recv_task_id:
                    return payload
                else:
                    print "Invalid task ID received"
            elif message_type == HEARTBEAT:
                pass
            else:
                print "Invalid message type received"
        if (datetime.now() - start_time).seconds > 5:
            break
        if (datetime.now() - prev_hb).seconds > 1:
            print "Sending heartbeat"
            socket.send_multipart(["", HEARTBEAT]) 
            prev_hb = datetime.now()
    return []


def test():  
    url = "tcp://localhost:5666"
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.connect(url)

    max = 2500
    for i in range(0, max):
        response = send_message(socket, ["some work from you"])
        print "Response: %s" % dump_message(response)

class Runner(threading.Thread):
    def __init__(self):
        super(Runner, self).__init__()

    def run(self):
        test()

if __name__ == "__main__":
    for i in range(0, 4):
        runner = Runner()
        runner.start()
