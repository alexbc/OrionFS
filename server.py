import socket
import select
import time

#create an INET, STREAMing socket
serversocket = socket.socket(
socket.AF_INET, socket.SOCK_STREAM)
#bind the socket to a public host, 
# and a well-known port
serversocket.bind(('127.0.0.1', 8010))
#become a server socket
serversocket.listen(5)
serversocket.setblocking(False)

sockets = [serversocket] #store the sockets we are looking at
files = [] #store the files we are looking at
downloads = [] #(socket, file) pairs for current file downloads

sockdata = {} #socket data buffer, to allow multipart commands
MAXPACKLEN = 8096 #max packet len (in bytes)
SOCKTIMEOUT = 10 #timeout time (seconds)

def closesock(sock): #close and remove all info on sock
    global downloads, scok, sockets

    print "Closing ", sockdata[sock]['address']
    del sockdata[sock]
    sockets.remove(sock)
    downloads = filter(lambda x: x[1] != sock, downloads)
    sock.close()
    return

def closefile(fle):
    global files, downloads
    print "Closing", fle.name
    files.remove(fle)
    downloads = filter(lambda x: x[0] != fle, downloads)

def malformedpack(buffer): #is buffer a malformed packet?
    # a valid packet is less then MAXPACKLEN bytes long, starts with "GET" and has exactly one space

    if len(buffer) > MAXPACKLEN:
        return True
    if buffer.split(" ")[0] != "GET":
        return True
    if buffer.count(" ") > 1:
        print buffer.split(" ")
        return True
    if buffer.count("\n") > 1:
        return True

    return False


def completedpack(buffer): #is this a completed packet?
    if buffer.count("\n") == 1:
        return True
    return False

def main():
    global files, downloads, sockets, sockdata, MAXPACKLEN, SOCKETTIMEOUT

    while 1:
        readl, writel, errorl = select.select(sockets, sockets, sockets, 0.1) #select and wait for up to 0.1 seconds before continuing to process
        readf, writef, errorf = select.select(files, files, files, 0.1) #select files that are avaliable, wait for up to 0.1 seconds before continuing to process

        print readl, writel, errorl

        for sock in errorl: #if socket is screwed up
            print "Error"
            closesock(sock) #Close it and remove all info


        for sock in readl: #do normal socket reading things
            if sock == serversocket: #if this is our accepting socket, we have a new connection request
                (clientsocket, address) = serversocket.accept() #get the request
                print "Connection from", address
                clientsocket.setblocking(False)
                sockets.append(clientsocket) #add it to our socket list
                sockdata[clientsocket] = {'buffer': '', 'address': address, 'lastrecv': time.time()} #add in the connectivity information
                continue #don't try and process it any more (accepting socket doesn't have any data to be read)

            data = sock.recv(4096) #recieve some data from the socket
            if len(data) == 0: #connection has been closed
                closesock(sock)
                continue

            sockdata[sock]['buffer'] += data #add the new data to our socket buffer
            buffer = sockdata[sock]['buffer']
            sockdata[sock]['lastrecv'] = time.time()

            if malformedpack(buffer): #if this packet is malformed, close the socket
               closesock(sock)
               continue

            if completedpack(buffer): #is this a completed packet? if so process it
                buffer = buffer.strip()
                payload = buffer.split(" ")[1]
                fp = open("cache/" + payload)
                print readf, writef, errorf
                print files
                files += [fp]
                downloads.append((fp, sock))

            print sockdata


        #now do file downloads
        finisheddownloads = []
        for sender, reciever in downloads:
            if sender in readf and reciever in writel:
                data = sender.read(8096)
                if len(data) == 0:
                    finisheddownloads += [(sender, reciever)]
                    continue
                else:
                    reciever.send(data)

        #clean up any file transfers that have finished
        for download in finisheddownloads:
            sender, reciever = download
            closesock(reciever)
            closefile(sender)

        #now look for sockets that have timedout
        now = time.time()
        deadsocks = []
        for sock, data in sockdata.iteritems():
            if now - data['lastrecv'] > SOCKTIMEOUT:
                deadsocks += [sock]

        for sock in deadsocks:
            closesock(sock)



if __name__ == "__main__":
    main()
    serversocket.close()
