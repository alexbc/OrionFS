import socket
import select

#create an INET, STREAMing socket
serversocket = socket.socket(
socket.AF_INET, socket.SOCK_STREAM)
#bind the socket to a public host, 
# and a well-known port
serversocket.bind(('127.0.0.1', 8010))
#become a server socket
serversocket.listen(5)
serversocket.setblocking(False)

sockets = [serversocket]
sockdata = {} #socket data buffer, to allow multipart commands
MAXPACKLEN = 8096

def closesock(sock): #close and remove all info on sock
    print "Closing ", sockdata[sock]['address']
    del sockdata[sock]
    sockets.remove(sock)
    sock.close()
    return

def malformedpack(buffer): #is buffer a malformed packet?
    if len(buffer) > MAXPACKLEN:
        return True
    if buffer.split(" ")[0] != "GET":
        return True
    if len(buffer.split) > 2:
        return True
    return False

def main():
    while 1:
        readl, writel, errorl = select.select(sockets, sockets, sockets)
        for sock in errorl: #if socket is screwed up
            print "Error"
            closesock(sock) #Close it and remove all info

        for sock in readl:
            if sock == serversocket: #if this is our accepting socket, we have a new connection request
                (clientsocket, address) = serversocket.accept() #get the request
                print "Connection from", address
                clientsocket.setblocking(False)
                sockets.append(clientsocket) #add it to our socket list
                sockdata[clientsocket] = {'buffer': '', 'address': address} #add in the connectivity information
                continue #don't try and process it any more (accepting socket doesn't have any data to be read)

            data = sock.recv(4096) #recieve some data from the socket
            if len(data) == 0: #connection has been closed
                closesock(sock)
                continue

            sockdata[sock]['buffer'] += data #add the new data to our socket buffer

            if malformedpack(sockdata[sock]['buffer']): #if this packet is malformed, close the socket
               closesock(sock)
               continue

            
            print sockdata



if __name__ == "__main__":
    main()
    serversocket.close()
