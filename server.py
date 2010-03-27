import socket
import select
import time
import os

#create an INET, STREAMing socket
serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #reuse sockets.

#bind the socket to a public host, 
# and a well-known port
serversocket.bind(('127.0.0.1', 8011))
#become a server socket
serversocket.listen(5)
serversocket.setblocking(False)

sockets = [serversocket] #store the sockets we are looking at
files = [] #store the files we are looking at

#TODO reconsile uploads and downloads
downloads = [] #(socket, file) pairs for current file downloads
uploads = [] #(file, socket) pairs for current file uploads


sockdata = {} #socket data buffer, to allow multipart commands
MAXPACKLEN = 8096 #max packet len (in bytes)
SOCKTIMEOUT = 120 #timeout time (seconds)

STATE_HANDSHAKE = 1
STATE_UPLOAD = 2
STATE_DOWNLOAD = 3

def closesock(sock): #close and remove all info on sock
    global downloads, scok, sockets

    print "Closing ", sockdata[sock]['address']
    del sockdata[sock]
    sockets.remove(sock)
    downloads = filter(lambda x: x[1] != sock, downloads)
    sock.close()
    return

def closefile(fle):
    global files, downloads, uploads
    print "Closing", fle.name
    files.remove(fle)
    downloads = filter(lambda x: x[0] != fle, downloads)
    uploads = filter(lambda x: x[1] != fle, uploads)
    fle.flush()
    fle.close()

def malformedpack(buffer): #is buffer a malformed packet?
    # a valid packet is less then MAXPACKLEN bytes long, starts with "GET" and has exactly one space

    if len(buffer) > MAXPACKLEN:
        return True
    if buffer.split(" ")[0] not in ["GET", 'PUT', 'RM'] and " " in buffer:
        return True
    if buffer.split("\n")[0].count(" ") > 1:
        return True

    return False


def completedpack(buffer): #is this a completed packet?
    if buffer.count("\n") >= 1:
        return True
    return False

def main():
    global files, downloads, uploads, sockets, sockdata, MAXPACKLEN, SOCKETTIMEOUT

    while 1:
        now = time.time()

        readl, writel, errorl = select.select(sockets, sockets, sockets, 0.1) #select and wait for up to 0.1 seconds before continuing to process
        readf, writef, errorf = select.select(files, files, files, 0) #select files that are avaliable now for reading/writing

        for sock in errorl: #if socket is screwed up
            print "Error"
            closesock(sock) #Close it and remove all info


        for sock in readl: #do normal socket reading things
            if sock == serversocket: #if this is our accepting socket, we have a new connection request
                (clientsocket, address) = serversocket.accept() #get the request
                print "Connection from", address
                clientsocket.setblocking(False)
                sockets.append(clientsocket) #add it to our socket list
                sockdata[clientsocket] = {'buffer': '', 'address': address, 'lastdata': time.time(), 'state': STATE_HANDSHAKE} #add in the connectivity information
                continue #don't try and process it any more (accepting socket doesn't have any data to be read)


            if sockdata[sock]['state'] == STATE_HANDSHAKE:
                data = sock.recv(8096) #recieve some data from the socket

                if len(data) == 0: #connection has been closed
                    closesock(sock)
                    continue

                sockdata[sock]['buffer'] += data #add the new data to our socket buffer
                buffer = sockdata[sock]['buffer']
                sockdata[sock]['lastdata'] = time.time()

                if malformedpack(buffer): #if this packet is malformed, close the socket
                    print "Malformed packet"
                    closesock(sock)
                    continue

                if completedpack(buffer): #is this a completed packet? if so process it
                    #TODO move to its own function
                    buffer = buffer.strip()
                    cmdline = buffer.split("\n")[0]
                    verb = cmdline.split(" ")[0]
                    
                    payload = ""
                    if " " in cmdline:
                        payload = cmdline.split(" ")[1]

                    if buffer.find("\n") == -1: #it had a newline at the end, so there is no extra info
                        buffer = ""
                    else:
                        buffer = buffer[buffer.find("\n") + 1:] #grab everything after the \n

                    if verb == "GET":
                        fp = open("cache/" + payload)
                        files += [fp]
                        downloads.append((fp, sock))
                        sock.shutdown(socket.SHUT_RD) #we are only sending data, so advertise that to the remote site
                        sockdata[sock]['state'] = STATE_DOWNLOAD

                    elif verb == "PUT":
                        fp = open("cache/" + payload, "w")
                        fp.write(buffer) #write the initial part of the buffer
                        files += [fp]
                        uploads.append((sock, fp))
                        #sock.shutdown(socket.SHUT_WR) #we are only reading data, so advertise that
                        sockdata[sock]['state'] = STATE_UPLOAD

                    elif verb == "LIST":
                        lst = os.listdir("cache/")
                        sock.send("\n".join(lst))
                        closesock(sock)

                    elif verb == "RM":
                        os.remove("cache/" + payload)
                        closesock(sock)

                    else:
                        closesock(sock)




        #now do file downloads
        #TODO make this more efficient (add additional selects/use filters)
        #TODO move this into its own sub
        curdownloads = filter(lambda x: x[0] in readf and x[1] in writel, downloads)
        for sender, reciever in curdownloads:
            sockdata[reciever]['lastdata'] = now
            data = sender.read(8096)
            if len(data) == 0: #transfer is complete
                closesock(reciever)
                closefile(sender)
            else:
                reciever.send(data)

        #now do file uploads
        #TODO make this more efficient
        #TODO make this its own sub
        curuploads = filter(lambda x: x[0] in readl and x[1] in writef, uploads)
        for sender, reciever in curuploads:
            sockdata[sender]['lastdata'] = now
            data = sender.recv(8096)
            if len(data) == 0: #transfer is complete
                closesock(sender)
                closefile(reciever)
                reciever.close()
            else:
                reciever.write(data)

        #TODO move this to its own subroutine
        #now look for sockets that have timedout
        checksocks = filter(lambda x: x != serversocket, sockets) #don't try and close the accepting socket
        deadsocks = filter(lambda x: now - sockdata[x]['lastdata'] > SOCKTIMEOUT, checksocks) #find those that haven't been touched in more then SOCKTIMEOUT seconds
        map(closesock, deadsocks) #and close them



if __name__ == "__main__":
    main()
    serversocket.close()
