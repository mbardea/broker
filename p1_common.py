import binascii

CL_TASK_AVAILABLE = "CL_TASK_AVAILABLE"
BR_HEARTBEAT = "BR_HEARTBEAT"
#HEARTBEAT = "HEARTBEAT"
CLIENT_HB = "CLIENT_HB"
TASK_CANCEL = "TASK_CANCEL"
BR_TASK_COMPLETE = "BR_TASK_COMPLETE"
WORKER_AVAILABLE = "WORKER_AVAILABLE"

class ParsingException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def dump_message(msg):
    def make_pretty(x):
        if len(x) == 17:
            return binascii.hexlify(x)
        else:
            return x
    part_len = map(make_pretty, msg)
    return "[" + "|".join(part_len) + "]"

def parse_message(msg, count):
    if not "" in msg:
        raise ParsingException("Message does not contain an empty part")
    idx = msg.index("")
    if idx == 0:
        router = None
    else:
        router = msg[idx - 1]

    if len(msg) - idx < count:
        raise ParsingException("Message does not have enough parts to parse")

    if count == 2:
        return (router, msg[idx + 1])
    if count == 4:
        return (router, msg[idx + 1], msg[idx + 2], msg[(idx + 3):])
    else:
        raise ParsingException("Bad request. Message count needs to be 2 or 4")
 
def parse_message_type(msg):
    prev_empty = False
    for part in msg:
        if len(part) == 0:
            prev_empty = True
            continue
        if prev_empty:
            return part
    return None

