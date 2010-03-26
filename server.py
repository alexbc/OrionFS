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

def closesock(sock):
    print "Closing ", sockdata[sock]['address']
    del sockdata[sock]
    sockets.remove(sock)
    sock.close()
    return

def main():
    while 1:
        readl, writel, errorl = select.select(sockets, sockets, sockets)
        for sock in errorl:
            print "Error"
            closesock(sock)


        for sock in readl:
            if sock == serversocket:
                (clientsocket, address) = serversocket.accept()
                print "Connection from", address
                clientsocket.setblocking(False)
                sockets.append(clientsocket)
                sockdata[clientsocket] = {'buffer': '', 'address': address}
                continue

            data = sock.recv(4096)
            if len(data) == 0: #connection has been closed
                closesock(sock)
                continue

            sockdata[sock]['buffer'] += data
            if len(sockdata[sock]['buffer']) >= MAXPACKLEN:
               closesock(sock)
               continue

            print sockdata



if __name__ == "__main__":
    main()
    serversocket.close()
