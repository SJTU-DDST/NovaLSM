import sys
import socket
import random
import argparse
import io


if __name__ == "__main__":
    strType = sys.argv[1]    
    
    if strType == "s":
        print("running server")    
        server = socket.socket()
        server.bind(('localhost', 8888))
        server.listen(1)
        print("waiting for connection")
        conn,addr = server.accept()
        print("connected")
        while(True):
            data = conn.recv(1024)
            if not data:
                print('client has ended connection')
                break
            print("get request from client")
            print("raw length: {}, raw data: {}, length: {}, data: {}".format(len(data), data, len(data.decode()), data.decode()))
            response = "0!1!1\n"
            conn.send(response.encode())
            print('send response to client')
            print("raw length: {}, raw data: {}, length: {}, data: {}".format(len(response), response, len(response.encode()), response.encode()))
        conn.close()
        
    elif strType == "c":
        print("running client")
        client = socket.socket()
        client.connect(('localhost',8888))
        print("connected")
        while(True):
            data = input('data to send to server: ')
            data = data + "\n"
            if len(data) == 0:
                continue
            print("raw length: {}, raw data: {}, length: {}, data: {}".format(len(data), data, len(data.encode()), data.encode()))
            client.send(data.encode())
            print("send request to server")
            response = client.recv(1024)
            print("get response from server")
            print("raw length: {}, raw data: {}, length: {}, data: {}".format(len(response), response, len(response.decode()), response.decode()))
        client.close()
        
    else:
        print("error type")
        sys.exit(0)
        
