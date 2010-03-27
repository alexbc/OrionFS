import socket
import select
import time
import os

MAXPACKLEN = 8096
SOCKTIMEOUT = 120

sockdata = {}
sockets = []

def callback(data):
    print "DATA!", data
    return


def closesock(sock):
    global sockdata, sockets
    if 'callback' in sockdata[sock]:
        sockdata[sock]['callback'](sockdata[sock]['inpbuffer'])

    del sockdata[sock]
    sockets.remove(sock)
    sock.close()

def gethost(fileaname, type): #only do one read
    #TODO actually work out what hosts to use based on consistant hashing, lol
    if type="r":
        return ('127.0.0.1', 8011)
    else
        return [('127.0.0.1', 8011)]

def buildsocket(filename, host):
    global sockdata, sockets
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(host)
    s.setblocking(False)
    sockets += [s]
    sockdata[s] = {}
    sockdata[s]['outbuffer'] = ''
    sockdata[s]['inpbuffer'] = ''
    sockdata[s]['putonly'] = False #if put only, after outbuffer is empty, the socket is closed

    return s

def getfile(filename, callback):
    global sockdata
    sock = buildsocket(filename, gethost(filename, "r"))
    sockdata[sock]['outbuffer'] = 'GET ' + filename + '\n'
    sockdata[sock]['callback'] = callback

def putfile(filename, file):
    global sockdata
    for host in gethost(filename, "w"):
        sock = buildsocket(filename, host)
        request = 'PUT ' + filename + '\n' + file
        sockdata[sock]['outbuffer'] = request
        sockdata[sock]['putonly'] = True


while 1:
    reads, writes, errors = select.select(sockets, sockets, sockets, 1)

    for sock in errors:
        print "Closing sock, error"
        closesock(sock)
        if sock in reads:
            reads.remove(sock)
        if sock in writes:
            writes.remove(sock)

    for sock in reads:
        data = sock.recv(8096) #recieve data
        print "Got data", data

        if len(data) == 0: #socket is closed
            print "closing socket"
            closesock(sock)
            if sock in writes:
                writes.remove(sock)
            continue

        sockdata[sock]['inpbuffer'] += data

    for sock in writes:
        if sockdata[sock]['outbuffer'] != "":
            print "Sending data", sockdata[sock]['outbuffer']
            bytessent = sock.send(sockdata[sock]['outbuffer']) #send data from output buffer
            sockdata[sock]['outbuffer'] = sockdata[sock]['outbuffer'][bytessent:] #remove bytes sent from buffer
        
            if sockdata[sock]['putonly']:
                closesock(sock)

