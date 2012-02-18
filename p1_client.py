#!/usr/bin/env python

import re, optparse, time, os, sys, threading, heapq
import zmq
from p1_common import *


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

url = "tcp://localhost:5666"
context = zmq.Context()
socket = context.socket(zmq.DEALER)
socket.connect(url)

socket.send("", zmq.SNDMORE)
socket.send("x", zmq.SNDMORE)
socket.send("yy")

msg = receive_within(socket, 1000, 5)
if msg:
    print "Received: %s" % dump_message(msg)
    print "Sending ping"
    socket.send("", zmq.SNDMORE)
    socket.send("ping")
    msg = receive_within(socket, 1000, 1)
    if msg:
        print "Received ping: %s" % dump_message(msg)
    else:
        print "Ping failed"

else:
    print "Failed to get a response"


