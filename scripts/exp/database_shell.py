import sys
import socket
import random
import argparse
import io


# def recv_send(server_ip, server_port):
#     # 1.创建socket对象，致命IPV4与TCP协议
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
 
#     # 2.绑定地址
#     s.bind((server_ip, server_port))
 
#     # 3.监听客户端的请求
#     s.listen()
 
#     # 4.接受客户端请求
#     conn,client_adr = s.accept()
 
#     # 5.接受/发送数据
#     while True:
#         # 接收数据
#         recv_data = conn.recv(1024).decode('utf-8')
#         print(f"接收:{recv_data}")
#         if recv_data == 'exit':       # 接收或者发送的是exit就退出
#             break
 
#         # 发送数据
#         send_data = input("发送：")
#         conn.send(send_data.encode('utf-8'))
#         if send_data == 'exit':
#             break
 
#     # 6.关闭连接
#     conn.close()
 
#     # 7.关闭套接字
#     s.close()

# nthreads=$1
##nova_servers=$2
##debug=$3
##partition=$4
##recordcount=$5
# maxexecutiontime=$6
# dist=$7
##value_size=${8}
# workload=${9}
##config_path=${10}
# cardinality=${11}
# operationcount=${12}
# zipfianconstant=${13}
# offset=${14}

class LTCFragment(object):
    def __init__(self):
        self.startKey = 0    
        self.endKey = 0
        self.ltcServerId = 0
        self.dbId = 0
        self.refCount = 0
        self.stocLogReplicaServerIds = []
    def toString(self):
        return "Fragment [start=" + str(self.startKey) + ", end=" + str(self.endKey) + ", serverIds=" + str(self.ltcServerId) + ", dcServerIds="+ str(self.stocLogReplicaServerIds) + "]"
    
class Configuration(object):
    def __init__(self):
        self.fragments = []
        self.ltcs = []
        self.stocs = []
        
class Configurations(object):
    def __init__(self):
        self.configs = []
        self.configurationId = 0
        self.servers = {}

class NovaClient(object):
    def __init__(self, servers, debug):
        self.sockets = []
        self.socketBuffer = list(" " * 1024 * 1024 * 2)
        self.servers = servers
        self.debug = debug
        self.sock_read_pivot = 0

        for i in range(0, len(servers)):
            host = servers[i]
            ip = host.split(":")[0]
            port = int(host.split(":")[1])
            print("Connecting to " + ip + ":" + str(port))
        
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, port))
            self.sockets.append(sock)
    
    def longToBytes(self, offset, x):
        len = 0
        p = offset 
        while(True):
            self.socketBuffer[offset + len] = chr(ord('0') + (x % 10))
            x = int(int(x) / 10)
            len = len + 1
            if x == 0:
                break
        q = offset + len - 1
        while p < q:
            temp = self.socketBuffer[p]
            self.socketBuffer[p] = self.socketBuffer[q]
            self.socketBuffer[q] = temp
            p = p + 1
            q = q - 1
        self.socketBuffer[offset + len] = "!"
        return len + 1
            
    def copyString(self, offset, value):     
        for i in range(0, len(value)):
            self.socketBuffer[offset + i] = value[i]    
    
    def readPlainText(self, sock, terminater):
        data = sock.recv(1024 * 1024 * 16)
        
        print("receive {}".format(data))
        #print(len(data))
        
        data = data.decode()
        
        # print(data)
        # print(len(data))
        
        if data[len(data)-1] == terminater:
            return data, len(data)
        else:
            Text = data
            while True:
                data = sock.recv(1)
                data = data.decode()
                Text = Text + data
                if data == terminater:
                    break
            return Text, len(Text)
         
    def bytesToLong(self, data):
        x = 0
        if data[self.sock_read_pivot] < '0' or data[self.sock_read_pivot] > '9':
            return x
        
        while(data[self.sock_read_pivot] >= '0' and data[self.sock_read_pivot] <= '9'):
            x = (ord(data[self.sock_read_pivot]) - ord('0')) + x * 10
            self.sock_read_pivot = self.sock_read_pivot + 1
            
        if ( data[self.sock_read_pivot] == '!'):
            self.sock_read_pivot = self.sock_read_pivot + 1
        
        return x;         
            
    def put(self, clientConfigId, key, value, serverId):
        getValue = None
        configId = 0
        
        self.sock_read_pivot = 0
        sock = self.sockets[serverId]
        intKey = int(key)
        self.socketBuffer[0] = 'p'
        size = 1
        size = size + self.longToBytes(size, clientConfigId)
        size = size + self.longToBytes(size, intKey)
        size = size + self.longToBytes(size, len(value))
        self.copyString(size, value)
        size = size + len(value)
        self.socketBuffer[size] = '\n'
        size = size + 1
        
        print("Put request: serverId:{} key:{}".format(serverId, intKey))
        print("content: {}".format(''.join(self.socketBuffer[0:size]).encode()))
        sock.send(''.join(self.socketBuffer[0:size]).encode())
        
        response, response_length = self.readPlainText(sock, '\n')
        # configId = 0
        
        # assert len(response) == 3
        # assert response[0] == '0'
        # assert response[1] == '!'
        # assert response[2] == '\n'
        
        # return getValue, configId

    def get(self, clientConfigId, key, serverId):
        getValue = None
        configId = 0
        
        self.sock_read_pivot = 0
        intKey = int(key)
        sock = self.sockets[serverId]
        self.socketBuffer[0] = 'g'
        cfgSize = self.longToBytes(1, clientConfigId)
        size = self.longToBytes(1 + cfgSize, intKey)
        self.socketBuffer[cfgSize + size + 1] = '\n'
        
        print("Get request: serverId:{} intKey:{}".format(serverId, intKey))
        print("content: {}".format(''.join(self.socketBuffer[0:cfgSize+size+2]).encode()))
        sock.send(''.join(self.socketBuffer[0:cfgSize+size+2]).encode())
        
        response, response_length = self.readPlainText(sock, '\n')
        # configId = self.bytesToLong(response)
        # valueSize = self.bytesToLong(response)
        # if valueSize == 0:
        #     return None, configId
        # try:
        #     getValue = response[self.sock_read_pivot:self.sock_read_pivot + valueSize]
        # except:
        #     print("Get {} failed!".format(intKey))
        
        # return getValue, configId
        
    def close(self):
        for sock in self.sockets:
            sock.close()
        return 

def readConfig(configPath):
    config = Configurations()
    try:
        file = open(configPath, 'r')
    except:
        print("err happened in opening configFile!")
        sys.exit(0)
    
    # lines = file.readlines()
    cfg = Configuration()
    # i = 0
    while True:
        line = file.readline()
        if line == "" or line == None:
            break           
        if "config" in line:
            if(len(cfg.fragments) != 0):
                config.configs.append(cfg)
            cfg = Configuration()
            line = file.readline()
            ltcs = line.split(",")
            for ltc in ltcs:
                cfg.ltcs.append(int(ltc))
            line = file.readline()
            stocs = line.split(",")
            for stoc in stocs:
                cfg.stocs.append(int(stoc))
            line = file.readline()
            # this line for ?
            continue
        
        ems = line.split(",")
        frag = LTCFragment()
        frag.startKey = int(ems[0])
        frag.endKey = int(ems[1])
        frag.ltcServerId = int(ems[2])
        frag.dbId = int(ems[3])
        for j in range(4, len(ems)):
            frag.stocLogReplicaServerIds.append(int(ems[j]))
        cfg.fragments.append(frag)
    
    config.configs.append(cfg)
    file.close()
    return config            

def homeFragment(key, config, partition):
    if partition == "hash":
        print("hash partition not supported")
        sys.exit(0)
    elif partition == "range":
        l = 0
        r = len(config) - 1
        m = int(l + int(r - l) / int(2))
        while(l <= r):
            m = int(l + int(r - l) / int(2))
            home = config[m]
            if(key >= home.startKey and key < home.endKey):
                break
            if key >= home.endKey:
                l = m + 1
            else:
                r = m - 1
        return m
    else:
        print("unknown partition not supported")
        sys.exit(0)

def generateRandomValue(value_size):
    output = io.StringIO()
    for i in range(0,value_size):
        output.write(chr(ord('a')+random.randint(0,26)))    
    result = output.getvalue()
    return result

def shellPut():
    pass

def shellGet():
    pass

def putAlot(novaClient, ops):
    for i in range(0, ops):
        novaClient.put(0, i, "a" * value_size, 0)
def getAlot(novaClient, ops):
    for i in range(0, ops):
        novaClient.get(0, i, 0)

def getSample(novaClient, maxkey):
    for i in range(0, int(maxkey / 1000)):
        novaClient.get(0, i * 1000, 0)
    
    
if __name__ == "__main__":
    
    # parse argument
    parser = argparse.ArgumentParser(description='PreLoader')
    parser.add_argument('--nova_servers', type=str, default="")
    parser.add_argument('--debug', type=str, default="false")
    parser.add_argument('--partition', type=str, default="range")
    parser.add_argument('--recordcount', type=int, default=10000000)
    parser.add_argument('--value_size', type=int, default=1024)
    parser.add_argument('--config_path', type=str, default="/home/yuhang/NovaLSM/config/nova-tutorial-config")
    
    print("parsing argument")
    args = parser.parse_args()
    nova_servers = args.nova_servers
    debug = args.debug
    partition = args.partition
    recordcount = args.recordcount
    value_size = args.value_size
    config_path = args.config_path
    
    print("args: ")
    print("nova_servers: ", nova_servers)
    print("debug: ", debug)
    print("partition: ", partition)
    print("recordcount: ", recordcount)
    print("value_size: ", value_size)
    print("config_path: ", config_path)
    sys.stdout.flush()
    
    # read config and settings
    
    print("reading config")
    config = readConfig(config_path)
    print("Number of fragments " + str(len(config.configs[config.configurationId].fragments)))
    sys.stdout.flush()
    
    servers = []
    serverIds = set()
    ems = nova_servers.split(",")
    for i in range(0, len(ems)):
        servers.append(ems[i])
        serverIds.add(i)
    
    # build connection
    
    print("connecting")
    novaClient = NovaClient(servers, debug)
        
    # preload
    
    # print("preloading")
    # keys = list(range(0, recordcount))
    # random.shuffle(keys)
    # i = 0
    # for key in keys:
    #     clientConfigId = config.configurationId
    #     current = config.configs[clientConfigId].fragments
    #     fragmentId = homeFragment(key, current, partition)
    #     serverId = current[fragmentId].ltcServerId
        
    #     value = generateRandomValue(value_size)
    #     getValue, configId = novaClient.put(clientConfigId, str(key), value, serverId)
        
    #     if i % 100000:
    #         print("i records inserted")
    #     sys.stdout.flush()
        
    # verify
    # print("verifying")
    # key = 0
    # clientConfigId = config.configurationId
    # current = config.configs[clientConfigId].fragments
    # fragmentId = homeFragment(key, current, partition)
    # serverId = current[fragmentId].ltcServerId
    # getValue, configId = novaClient.get(clientConfigId, str(key), serverId)
    # if getValue != None and configId == clientConfigId:
    #     print("key 0 verified")
    # else:
    #     print("key 0 verification failed")
    #     sys.exit(0)
        
    # key = recordcount - 1
    # clientConfigId = config.configurationId
    # current = config.configs[clientConfigId].fragments
    # fragmentId = homeFragment(key, current, partition)
    # serverId = current[fragmentId].ltcServerId
    # getValue, configId = novaClient.get(clientConfigId, str(key), serverId)
    # if getValue != None and configId == clientConfigId:
    #     print("key {} verified".format(recordcount - 1))
    # else:
    #     print("key {} verification failed".format(recordcount - 1))
    #     sys.exit(0)
    
    
    
    # server_ip = '127.0.0.1'
    # server_port = 5555
    # # recv_send(server_ip, server_port)
    
    # print("generating random insertion sequence...")
    
    # random.shuffle(range(0,recordcount + 1))
    
    # print("socket creating...")
    
    # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # print("connecting ... ")
    
    # sock.connect((address, port))
    
    # print("")
    
    while(True):
        strCommand = input("command: ")
        command = strCommand.strip("\n").split(" ")
        if len(command) <= 0 or len(command) >= 4:
            print("incorrect command")
            continue
        elif len(command) == 1:
            if command[0] == "end":
                print("end")
                break
            elif command[0] == "gs":
                getSample(novaClient, int(recordcount))
            else:
                print("incorrect command")
                continue
        elif len(command) == 2:
            if command[0] == "get" or command[0] == "g":
                key = command[1]
                novaClient.get(0, key, 0)
                # if getValue == None:
                #     print("key {} not found".format(key))
                # else:
                #     print("getValue: {}, configId:{}".format(getValue, configId))
            elif command[0] == "getalot":
                ops = int(command[1])
                getAlot(novaClient, ops)
            elif command[0] == "putalot":
                ops = int(command[1])
                putAlot(novaClient, ops)
            else:
                print("incorrect command")
                continue
        elif len(command) == 3:
            if command[0] == "put" or command[0] == "p":
                key = command[1]
                novaClient.put(0, key, "a" * value_size, 0)
                # print("getValue: {}, configId:{}".format(getValue, configId))
            else:
                print("incorrect command")
                continue                
    
    novaClient.close()
    # print("preload success!")

    # 关闭socket连接

