import socket
import hashlib
import random
import time

PEERS = [('localhost', 8011)]
QUORUM = 3

def findpeers(peerlist, key):
    hash = lambda x: int(hashlib.sha512(x).hexdigest(), 16)
    
    hashofkey = hash(key)

    peerlist.sort(key=lambda x: hash(x[0]))
    lowerlist = filter(lambda x: x < hashofkey, peerlist)
    upperlist = filter(lambda x: x > hashofkey, peerlist)

    return upperlist + lowerlist



def getfromhost(key, host):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(host)
    s.send("GET %s\n" % key)
    buff = ""
    while 1:
        cur = s.recv(8096)
        if len(cur) == 0:
            break
        buff += cur
    return buff

def puttohost(key, host, value):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(host)
    s.send("PUT %s\n" % key)
    while value != "":
        ret = s.send(value)
        value = value[ret:]

    time.sleep(1)
    s.close()
    return

def rmfromhost(key, host):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(host)
    s.send("RM %s\n" % key)
    s.close()
    return

def getblock(key):
    host = random.choice(findpeers(PEERS, key)[:3])
    val = getfromhost(key, host)
    print "%s = %s" % (key, val)
    return val

def putblock(key, value):
    print "Set %s = %s" % (key, value)
    hosts = findpeers(PEERS, key)[:3]
    for host in hosts:
        puttohost(key, host, value)

def rmblock(key):
    print "Rm %s" % key
    hosts = findpeers(PEERS, key)[:3]
    for host in hosts:
        rmfromhost(key, host)

if __name__ == "__main__":
    putblock("test", "test\n")
    print getblock("test")
#    rmblock('test')

