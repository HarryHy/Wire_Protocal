import socket
import json
import pickle
import fnmatch
import uuid
import queue
import threading
import os
import os.path
import sys

lock = threading.Lock()
messages = queue.Queue()
users = []

"""Server for multithreaded (asynchronous) chat application in distributed server supported by Raft Algorithm."""

from socket import *
from threading import Thread
import sys
import json
import random
from threading import Timer
import numpy as np
import os
import time
import fnmatch
#import socket

#
#
#   msg: {'REQ_VOTE'}
#        {'REQ_VOTE_REPLY'}
#        {'ClientRequest'}
#        {'AppendEntry'}
#        {'AppendEntryConfirm'}
#   Log status:         # Uncommit Commited Applied
#
#   AppendEntries RPC: HeartBeat
#   # CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit
#
#   RPC Reply: CurrentTerm, Success
#
#
#

class delete_account_exception(Exception):
    def __init__(self, message):
        print(message)

def get_available_port(ip_address, start_port=5000, end_port=6000):
    print("get_available_port")
    for port in range(start_port, end_port):
        try:
            sock = socket(AF_INET, SOCK_STREAM)
            sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1) 
            sock.bind((ip_address, port))
            return port
        except error as e:
            if e.errno == 48:  # Address already in use
                continue
            else:
                raise e
        finally:
            print("now")
            sock.close()
    raise Exception(f"No available port found between {start_port} and {end_port}")


class Server:
    def __init__(self, server_id, CONFIG):
        self.init_server_variables(server_id, CONFIG)
        
        self.server = socket(AF_INET, SOCK_STREAM)
        self.server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1) 
        self.server.bind((self.HOST, self.server_port[self.server_id]['port']))
        print("server  is ", self.server)
        self.server.listen(10)
        
        #available_server_port = get_available_port(self.HOST, 32350, 32372)
        #print("available_server_port ", available_server_port)
        #self.server_port[self.server_id]['server_port'] = available_server_port
        self.listener = socket(AF_INET, SOCK_DGRAM)
        self.listener.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1) 
        self.listener.bind((self.HOST, self.server_port[self.server_id]['server_port']))
        print("listner is ", self.listener)
        #self.listener.listen(10)
        # the interval used by the leader to send out regular heartbeat messages to the followers
        # to ensure that the followers are still connected to the leader and 
        # are aware of its existence
        # if the leader fails to send out a heartbeat message within the timeout period
        # the follower assume the leader is not functioning and starts a new election

        # the timeout value that is used by followers and candidates to trigger a new election
        self.election_timeout = random.uniform(self.timeout, 1.2 * self.timeout)

        # become candidate after timeout
        
        self.heartbeat_timer = None

        self.election_timer = Timer(self.election_timeout, self.start_election)
        self.election_timer.daemon = True
        self.election_timer.start()
        print("finish")
        self.clients = {} 
        self.logins = set()
        self.LOGIN_LIMIT = 5

        self.login_times = 0
        threading.Thread(target=self.start())
        threading.Thread(target=self.rec_msg())


    def init_server_variables(self, server_id, CONFIG):
            self.server_id = server_id
            self.leader_id = None
            json.dump(CONFIG, open('config.json', 'w'))
            self.server_port = CONFIG['server_port']
            self.clients_con = []
            self.clients_con_map = {}
            self.addresses = {}
            self.HOST = 'localhost'
            self.BUFSIZ = 1024
            self.log = [{'Content': '', 'term': 0, 'index': 0}]
            self.CommitIndex = 0
            self.LastApplied = 0
            self.nextIndices = {}
            self.loggedIndices = {}

            self.current_term = 0
            self.timeout = 2

            json.dump(CONFIG, open('config.json', 'w'))

            self.heartbeat_timeout = 1
            self.role = 'follower'

            self.votes = {}
            self.vote_log = {}

    # handleIncommingMessage
    def handleIncommingMessage(self, msg):
        # handle incomming messages
        # Message types:
        # messages from servers
        # 1. requestVote RPC
        msg_type = msg['Command']
        if msg_type == 'REQ_VOTE':
            self.handleRequestVote(msg)
        # 2. requestVoteReply RPC
        elif msg_type == 'REQ_VOTE_REPLY':
            self.handleRequestVoteReply(msg)
        # 3. deal with clients
        elif msg_type == 'ClientRequest':
            self.handleClientRequest(msg)
        # 4. append entry
        elif msg_type == 'AppendEntry':
            self.CommitEntry(msg)
        # 5. Confirm append entry
        elif msg_type == 'AppendEntryConfirm':
            self.handleAppendEntryReply(msg)

    def start(self):
        print("Waiting for connection...")
        self.new_thread = Thread(target=self.accept_incoming_connections)
        self.new_thread.start()

    # start election
    def start_election(self):
        """
                start the election process
        """
        print('start election')
        self.role = 'candidate'
        self.leader_id = None
        self.resetElectionTimeout()
        self.current_term += 1
        self.votes[self.current_term] = self.server_id
        self.vote_log[self.current_term] = [self.server_id]

        print('become candidate for term {}'.format(self.current_term))
        # handle the case where only one server is left
        if not self.isLeader() and self.enoughForLeader():
            self.becomeLeader()
            return
        # send RequestVote to all other servers
        # (index & term of last log entry)
        #reset the election_tiimeout 
        self.election_timeout = random.uniform(self.timeout, 1.2 * self.timeout)
        self.requestVote()

    def resetElectionTimeout(self):
        """
        reset election timeout
        """
        if self.election_timer:
            self.election_timer.cancel()
        # need to restart election if the election failed
        # print('reset ElectionTimeout')
        self.election_timer = Timer(self.election_timeout, self.start_election)
        self.election_timer.daemon = True
        self.election_timer.start()

    def rec_msg(self):
        while True:
            try:
                msg, address = self.listener.recvfrom(4096)
                msg = json.loads(msg)
                self.handleIncommingMessage(msg)
            except KeyboardInterrupt:
                print("Quit the server")
                server_id = self.server_id
                CONFIG = json.load(open("config.json"))
                CONFIG['server_on'].remove(server_id)
                json.dump(CONFIG, open('config.json', 'w'))
                os._exit(0)
                # os.system('python3 state_ini.py 5')

    # CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit, server_id, Command
    def handleClientRequest(self, msg):
        # term = msg['current_term']
        # if term < self.current_term:
        #     pass
        #     # self.clientRequestReply(msg, False)
        # serverId = msg['server_id']
        # self.nextIndices[serverId] = msg['CommitIndex']
        # self.log.append(msg['Entries'])
        # msg = {'Command': 'ClientRequest', 'Content': content, 'term'}

        del msg['Command']
        self.log.append(msg)
        print('handle client request')
        self.CommitIndex += 1
        self.LastApplied += 1
        self.broadcast_client(msg['Content'])
        self.sendHeartbeat()


    def requestVote(self):
        # broadcast the request Vote message to all other datacenters
        message = {'Command': 'REQ_VOTE', 'ServerId': self.server_id, 'current_term': self.current_term,
                   'log_len': len(self.log)}
        CONFIG = json.load(open("config.json"))
        self.server_port = CONFIG['server_port']
        server_on_list = CONFIG['server_on']
        for server_id in self.server_port:
            if server_id != self.server_id and server_id in server_on_list:
                self.sendMessage(server_id, message)

        # delay
        # Timer(CONFIG['messageDelay'], sendMsg).start()

    # new_add
    def handleRequestVote(self, msg):
        """
        Handle incoming requestVote message
        :type candidate_id: str
        :type candidate_term: int
        :type candidate_log_term: int
        :type candidate_log_index: int
        """
        candidate_term = msg['current_term']
        candidate_id = msg['ServerId']

        # if candidate term < current term reject the vote
        if candidate_term < self.current_term:
            self.requestVoteReply(candidate_id, False)
            return

        self.current_term = max(candidate_term, self.current_term)
        approve = False

        if self.current_term not in self.vote_log:
            self.vote_log[self.current_term] = [candidate_id]
            approve = True
            self.role = 'follower'

        if msg['log_len'] < len(self.log):
            approve = False

        # can only vote for one candidate at one term
        if candidate_term in self.votes:
            if self.votes[candidate_term] != candidate_id:
                approve = False
        print("election self.vote_log, ", self.vote_log)
        print("who am I ", self.server_id)
        self.requestVoteReply(candidate_id, approve)

    def handleRequestVoteReply(self, msg):
        """
        handle the reply from requestVote RPC
        :type follower_id: str
        :type follower_term: int
        :type approve: bool
        """
        print("handleRequestVoteReply")
        follower_id = msg['server_id']
        follower_term = msg['current_term']
        approve = msg['Decision']

        if approve:
            self.vote_log[self.current_term].append(follower_id)
            print('get another vote in term {}, votes got: {}'.format(self.current_term,
                                                                      self.vote_log[self.current_term]))
        if not self.isLeader() and self.enoughForLeader():
            self.becomeLeader()
        else:
            print("no")
            if follower_term > self.current_term:
                self.current_term = follower_term
                self.change_to_follower(follower_term)

    def change_to_follower(self, follower_term):
        print('update itself to term {}'.format(self.current_term))
        # if candidate or leader, step down and acknowledge the new leader
        if self.isLeader():
            self.heartbeat_timer.cancel()
        self.leader_id = None
        # need to restart election if the election failed
        self.resetElectionTimeout()
        # convert to follower, not sure what's needed yet
        self.role = 'follower'
        self.current_term = follower_term
        self.vote_log[self.current_term] = []

    def becomeLeader(self):
        """
        do things to be done as a leader
        """
        print('become leader for term {}'.format(self.current_term))
        #self.broadcast_client("SERVERINFO:After election, server %s becomes leader"%self.server_id)
        text = 'SERVERINFO:After election, server %s becomes leader'%self.server_id
        msg = {'Content': text, 'term': self.current_term, 'index': len(self.log)}
        for log in self.log:
            if log['Content'] == text:
                msg = {'Content': 'SERVERINFO:After election, new server %s becomes leader'%self.server_id, 'term': self.current_term, 'index': len(self.log)}
                break
        self.log.append(msg)
        self.CommitIndex += 1
        self.LastApplied += 1
        self.broadcast_client(msg['Content'])
        # no need to wait for heartbeat anymore
        self.election_timer.cancel()

        self.role = 'leader'
        self.leader_id = self.server_id
        CONFIG = json.load(open("config.json"))
        server_on_list = CONFIG['server_on']
        # initialize a record of nextIdx
        self.nextIndices = dict([(server_id, len(self.log) - 1)
                                 for server_id in server_on_list
                                 if server_id != self.server_id])
        # print('1nextIndices',self.nextIndices)
        print('send heartbeat')
        self.sendHeartbeat()
        self.heartbeat_timer = Timer(self.heartbeat_timeout, self.sendHeartbeat)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

    def sendHeartbeat(self):
        """
        Send heartbeat message to all pears in the latest configuration
        if the latest is a new configuration that is not committed
        go to the join configuration instead
        :type ignore_last: bool
              - this is used for the broadcast immediately after a new
              config is committed. We need to send not only to sites
              in the newly committed config, but also to the old ones
        """

        CONFIG = json.load(open("config.json"))
        self.server_port = CONFIG['server_port']
        server_on_list = CONFIG['server_on']

        for server_id in self.server_port:
            if server_id != self.server_id and server_id in server_on_list:
                # print("11",server_id)
                if server_id not in self.nextIndices:
                    self.nextIndices[server_id] = 0
                # print("22", server_id)
                # print('2nextIndices', self.nextIndices)
                self.sendAppendEntry(server_id)

        self.resetHeartbeatTimeout()


    def resetHeartbeatTimeout(self):
        """
        reset heartbeat timeout
        """
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = Timer(self.heartbeat_timeout, self.sendHeartbeat)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

    # CurrentTerm, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit, server_id, Command

    def appendEntry(self, target_id, prev_log_idx,
                    prev_log_term, entries):
        msg = {'Command': 'AppendEntry', 'current_term': self.current_term, 'PrevLogIndex': prev_log_idx,
               'PrevLogTerm': prev_log_term, 'Entries': entries, 'LeaderCommit': self.CommitIndex,
               'LeaderId': self.server_id}
        # print('send entry heartbeat %s' % entries)
        # print('this the msg',msg)
        self.sendMessage(target_id, msg)

    def sendAppendEntry(self, server_id):
        """
        send an append entry message to the specified datacenter
        :type center_id: str
        """
        max_num = len(self.log)
        if self.nextIndices[server_id] - 1 >= 0:
           # print('1serverod',server_id)
           # print("2test",self.nextIndices[server_id])
           # print("3test11",self.log)
            prevEntry = self.log[self.nextIndices[server_id] - 1]

        else:
            prevEntry = self.log[0]

        # print(self.nextIndices)
        # print(self.CommitIndex)
        # print(prevEntry)

        self.appendEntry(server_id, prevEntry['index'], prevEntry['term'], self.log[self.nextIndices[server_id]])




    # msg = {'Command': 'AppendEntry', 'current_term': self.current_term, 'PrevLogIndex': prev_log_idx,
    #        'PrevLogTerm': prev_log_term, 'Entries': entries, 'CommitIndex': self.CommitIndex}
    def CommitEntry(self, msg):
        # print(msg)
        # print(msg['LeaderId'])
        # print('send commit entry')
        self.leader_id = msg['LeaderId']
        term = msg['current_term']
        self.current_term = term if self.current_term < term else self.current_term
        self.resetElectionTimeout()
        msg['server_id'] = self.server_id
        msg['Command'] = 'AppendEntryConfirm'
        if msg['Entries'] in self.log:
            msg['Confirm'] = 'AlreadyGot'
            self.sendMessage(self.leader_id, msg)
            return
        print('append %s' % msg['Entries'])
        msg['Confirm'] = 'Success'
        self.sendMessage(self.leader_id, msg)
        self.log.append(msg['Entries'])
        self.CommitIndex += 1
        self.broadcast_client(msg['Entries']['Content'])

    # msg = {'Command': 'Append', 'current_term': self.current_term, 'PrevLogIndex': prev_log_idx,
    #        'PrevLogTerm': prev_log_term, 'Entries': entries, 'CommitIndex': self.CommitIndex}
    def handleAppendEntryReply(self, msg):
        """
        handle replies to appendEntry message
        decide if an entry can be committed
        :type follower_id: str
        :type follower_term: int
        :type success: bool
        :type follower_last_index: int
        """
        follower_id = msg['server_id']
        follower_term = msg['current_term']
        success = msg['Confirm']
        # if success=='AlreadyGot':
        #     follower_last_index = msg['PrevLogIndex']
        # else:
        if follower_term > self.current_term:
            self.change_to_follower(follower_term)
            return

        # if I am no longer the leader, ignore the message
        if not self.isLeader(): return
        # if the leader is still in it's term
        # adjust nextIndices for follower
        # print(msg)
        # print(self.nextIndices[follower_id])
        # print(follower_last_index)
        # print(self.CommitIndex)

        if self.nextIndices[follower_id] != self.CommitIndex:
            #print('self.commitIndex',self.CommitIndex)
            self.nextIndices[follower_id] += 1
            print('update nextIndex of {} to {}'.format(follower_id, self.nextIndices[follower_id]))

        if success == 'AlreadyGot':
            # self.sendAppendEntry(follower_id)
            # print('already got that entry')
            return

        # find out the index most followers have reached
        majority_idx = self.maxQualifiedIndex(self.nextIndices)
        print('the index logged by majority is {0}'.format(majority_idx))
        # commit entries only when at least one entry in current term
        # has reached majority
        if self.log[majority_idx]['term'] != self.current_term:
            print('term no right')
            return
        # if we have something to commit
        # if majority_idx < self.CommitIndex, do nothing
        # old_commit_idx = self.CommitIndex
        # self.CommitIndex = max(self.CommitIndex, majority_idx)
        # TODO
        # list(map(self.commitEntry, self.log[old_commit_idx + 1:majority_idx + 1]))

    def maxQualifiedIndex(self, indices):
        """
        Given a dictionary of datacenters and the max index in their log
        we find of the maximum index that has reached a majority in
        current configuration
        """
        # entry = self.getConfig()
        # the leader keep its own record updated to the newest
        indices[self.server_id] = len(self.log) - 1
        # print('!!!!!', indices)
        # if entry['config'] == 'single':
        #     return sorted([indices[x] for x in entry['data']])[int((len(entry['data'])-1)/2)]
        # maxOld = sorted([indices[x] for x in entry['data'][0]])[int((len(entry['data'][0])-1)/2)]
        # maxNew = sorted([indices[x] for x in entry['data'][1]])[int((len(entry['data'][1])-1)/2)]

        return min(indices.values())

    def enoughForLeader(self):
        """
        Given a list of servers who voted, find out whether it
        is enough to get a majority based on the current config
        :rtype: bool
        """
        CONFIG = json.load(open("config.json"))
        server_on_list = CONFIG['server_on']
        print('enough for leader? %s > %s' % (
            np.unique(np.array(self.vote_log[self.current_term])).shape[0], len(server_on_list) / 2))
        return np.unique(np.array(self.vote_log[self.current_term])).shape[0] > len(server_on_list) / 2

    def isLeader(self):
        """
        determine if the current server is the leader
        """
        return self.server_id == self.leader_id

    # new_add
    def requestVoteReply(self, target_id, approve):
        # send reply to requestVote message
        message = {'Command': 'REQ_VOTE_REPLY', 'server_id': self.server_id, 'current_term': self.current_term,
                   'Decision': approve}
        self.sendMessage(target_id, message)

    # Timer(CONFIG['messageDelay'], sendMsg).start()

    # new_add
    def sendMessage(self, server_id, message):
        """
        send a message to the target server
        should be a UDP packet, without gauranteed delivery
        :type target_meta: e.g. { "port": 12348 }
        :type message: str
        """
        message = json.dumps(message)
        peer_socket = socket(AF_INET, SOCK_DGRAM)
        # print('server_id %s' % server_id)
        # print('leader_id %s' % self.leader_id)
        #########
        port = self.server_port[server_id]['server_port']
        addr = (self.HOST, port)
        peer_socket.sendto(message.encode(), addr)

        # peer_socket.connect(addr)
        # self.all_socket[port].send(message)

    def accept_incoming_connections(self):
        """Sets up handling for incoming clients."""
        print("accept incoming connecgtion")
        self.name = None
        while True:
            
            client, client_address = self.server.accept()
            print("client is ", client)
            self.clients_con.append(client)
            print("%s:%s has connected." % client_address)
            #client.send(bytes("Welcome! You are at server %s. Type your username and press enter to continue."%self.server_id, "utf8"))
            self.addresses[client] = client_address
            threading.Thread(target=self.handle_client, args=(client,)).start()

    def rec_client(self, content):
        print("line 590 content is ", content)
        print('receive client request')
        msg = {'Command': 'ClientRequest', 'Content': content, 'term': self.current_term, 'index': len(self.log)}
        if self.server_id != self.leader_id:
            print(' Transfer to leader')
            self.sendMessage(self.leader_id, msg)
        else:
            print('log record client')
            del msg['Command']
            self.log.append(msg)
            print(self.log)
            self.CommitIndex += 1
            self.LastApplied += 1
            self.broadcast_client(content)
            self.sendHeartbeat()
    
    def rec_logins(self, logins_set):
        print("line 605 content is")
        print('receive update logs request')
        msg = {'Login': 'LogRequest', 'Content': self.logins, 'term': self.current_term, 'index': len(self.log)}
        if self.server_id != self.leader_id:
            print(' Transfer to leader')
            self.sendMessage(self.leader_id, msg)
        else:
            print('log record client')
            del msg['Login']
            self.log.append(msg)
            print(self.log)
            self.CommitIndex += 1
            self.LastApplied += 1
            #self.broadcast_client(content)
            self.sendHeartbeat()


    def loginSuccess(self, client, username, old_name):
        welcome = 'Welcome %s! If you want to quit, type {quit} to exit.' % username
        client.send(bytes("LOGINSUCCESS" + welcome, 'utf8'))
        msg = "%s has joined the chat!" % username
        # add to the list of login user in the accounts file
        self.clients_con_map[username] = client

        CONFIG = json.load(open("config.json"))
        if old_name is not None:
            CONFIG['clients_loggedin'].remove(old_name)
        if username not in CONFIG['clients_loggedin']:
            CONFIG['clients_loggedin'].append(username)
        json.dump(CONFIG, open('config.json', 'w'))

        for log in self.log:
            time.sleep(0.1)
            client.send(bytes(log['Content'], 'utf8'))
        self.rec_client(msg)

    def login(self, client, operation, name, old_name):
        if operation == "SIGNUP":
            username = name.split("+")[0]
            password = name.split("+")[1]
            with open('accounts.json') as f:
                data = json.load(f)
            if username in data:
                client.send(bytes("DUPNAME", 'utf8'))
                return None
            else:
                # no duplicate name, create this account
                with open("accounts.json", "w") as f:
                    data[username] = {"password": password}
                    json.dump(data, f, indent=4)
                # login success
                self.loginSuccess(client, username, old_name)
                return username
        elif operation == "LOGIN":
            splited = name.split("+")
            if len(splited) <= 1:
                client.send(bytes("INVALID", 'utf8'))
                return None
            username = splited[0]
            password = splited[1]
            with open('accounts.json') as f:
                data = json.load(f)
            if username not in data:
                client.send(bytes("NOUSER", 'utf8'))
                return None
            else:
                if data[username]['password'] != password:
                    client.send(bytes("WRONGPW", 'utf8'))
                    return None
                else:
                    self.loginSuccess(client, username, old_name)
                    return username
        else:
            print("operation is ", operation)
            self.rec_client("you are not login, it is invalid operation!")
            return None

    def listAccounts(self, client, pattern):
        with open('accounts.json') as f:
            data = json.load(f)
        if pattern == "ALL":
            keys = list(data.keys())
        else:
            keys = []
            for k in list(data.keys()):
                if fnmatch.fnmatch(k, pattern):
                    keys.append(k)
        if len(keys) == 0:
            client.send(bytes("NOMATCHED", 'utf8'))
        else:
            accounts = ""
            for k in keys:
                accounts += k + " "
            client.send(bytes("MATCHED" + accounts, 'utf8'))

    def talkto(self, content, name, client):
        username_talkto = content.split("+")[0]
        content_talkto = content.split("+")[1]
        ACCOUNTS = json.load(open("accounts.json"))
        if username_talkto in ACCOUNTS:
            HIST = json.load(open("offline_messages.json"))
            if username_talkto in HIST:
                if name in HIST[username_talkto]:
                    for msg in HIST[username_talkto][name]:
                        client.send(bytes("QUEUED" + username_talkto + " to " + name + ": " + msg, 'utf8'))
                        self.rec_client("DEQUEUED: " + username_talkto + " to " + name + ": " + msg) # add to log
                    del HIST[username_talkto][name]
                    json.dump(HIST, open('offline_messages.json', 'w'))
            CONFIG = json.load(open("config.json"))
            if username_talkto in CONFIG["clients_loggedin"]:
                # shown in talkto's GUI
                self.clients_con_map[username_talkto].send(bytes(name + " to " + username_talkto + ": " + content_talkto, 'utf8'))
                # shown in user's GUI
                if username_talkto != name:
                    client.send(bytes(name + " to " + username_talkto + ": " + content_talkto, 'utf8'))
                # add to log
                self.rec_client("LIVE: " + name + " to " + username_talkto + ": " + content_talkto)
            else:
                client.send(bytes("OFFLINE" + username_talkto, 'utf8'))
                HIST = json.load(open("offline_messages.json"))
                if name not in HIST:
                    HIST[name] = {username_talkto:[content_talkto]}
                elif username_talkto not in HIST[name]:
                    HIST[name][username_talkto] = [content_talkto]
                else:
                    HIST[name][username_talkto].append(content_talkto)
                json.dump(HIST, open('offline_messages.json', 'w'), indent=4)
                self.rec_client("QUEUED: " + name + " to " + username_talkto + ": " + content_talkto)
        else:
            client.send(bytes("INVALTALKTO" + username_talkto, 'utf8'))
            return

    def handle_client(self, client):  # Takes client socket as argument.
        """Handles a single client connection."""
        #name = None
        #print(f"Connected with {str(addr)}")
        username = ""
        talkto = ""
        print("in handle client ")
        
        try:
            while True:
                
                operation = client.recv(1024).decode('ascii')
                print("operation is ", operation)
                print("line 733")
                self.rec_client(operation)
                # If the request is a "LOGIN" request, it checks if the user has exceeded the login limit. 
                # If the user has exceeded the limit, it sends a "FAIL" message to the client and closes the connection. 
                # Otherwise, it prompts the user to enter their username and password, checks if the username and password are correct, and either logs the user in or rejects the login attempt.
                if operation == "LOGIN":
                    self.login_times += 1
                    print("logging...")
                    if self.login_times > self.LOGIN_LIMIT:
                        client.send("FAIL".encode('ascii'))
                        client.close()

                    # check whether username and password match
                    # ask the client for the username
                    client.send("USERNAME".encode('ascii'))
                    username = client.recv(1024).decode('ascii')
                    
                    client.send('PASSWORD'.encode('ascii'))
                    password = client.recv(1024).decode('ascii')
                    
                    # check if password matches with user name
                    with open('accounts.json') as f:
                        data = json.load(f)
                    if username not in data.keys():
                        print("No such user")
                        client.send("NOUSER".encode('ascii'))
                    elif password != data[username]["password"]:
                        print("Login rejected!")
                        client.send("REJECT".encode('ascii'))
                        #continue # returns the control to the beginning of the while loop
                    else:
                        print("Successfully logged in! as ", username)
                        client.send("ACCEPT".encode('ascii'))
                        self.clients[username] = client
                        print("add " + username + " to logins")
                        self.rec_client(username)

                
                # If the request is a "SIGNUP" request, it checks if the requested username is already taken. 
                # If the username is available, it prompts the user to enter a password, generates a unique ID for the new account, and stores the account information in a JSON file.
                elif operation.startswith("SIGNUP"):
                    # check whether the username already exists
                    username = operation.split(" ")[1]
                    with open('accounts.json') as f:
                        data = json.load(f)
                    if username in data:
                        client.send('DUPNAME'.encode('ascii'))
                    else:
                        client.send('NONDUPNAME'.encode('ascii'))
                        password = client.recv(1024).decode('ascii')
                        # generate a unique uuid for the new account
                        id = uuid.uuid1()
                        # store the new created account into the json file
                        with open("accounts.json", "w") as f:
                            data[username] = {"password": password}
                            json.dump(data, f, indent=4)
                    self.rec_client(operation + username)
                # If the request is a "LIST" request, it searches for usernames that match a given pattern in a JSON file containing account information. 
                # If there are matches, it sends a list of the matching usernames to the client.
                elif operation.startswith("LIST"):
                    pattern = operation.split(" ")[1]
                    with open('accounts.json') as f:
                            data = json.load(f)
                    if pattern == "ALL":
                        keys = list(data.keys())
                    else:
                        keys = []
                        for k in list(data.keys()):
                            if fnmatch.fnmatch(k, pattern):
                                keys.append(k)
                    if len(keys) == 0:
                        client.send("NOMATCHED".encode('ascii'))
                    else:
                        client.send("MATCHED".encode('ascii'))
                        next_line = client.recv(1024).decode('ascii')
                        if next_line == "SENDMATCHED":
                            lists = pickle.dumps(keys)
                            client.send(lists)   

                # If the request is a "TALKTO" request, it checks if the requested username is valid. 
                # If the username is valid, it adds the user to a set of logged-in users.
                elif operation.startswith("TALKTO"):
                    talkto = operation.split(" ")[1]
                    # check if the talkto username is valid
                    with open('accounts.json') as f:
                        data = json.load(f)
                    if talkto in data:
                        print(talkto + " is a valid user")
                        client.send("VALTALKTO".encode('ascii'))
                        self.logins.add(username)
                        self.rec_logins()
                    else:
                        client.send("INVALTALKTO".encode('ascii'))
                        # lists = pickle.dumps(list(data.keys()))
                        # client.send(lists)
                # If the request is a "STARTHIST" request, it sends any queued messages from a previous chat session with a user to the current user.
                elif operation == "STARTHIST":
                    # send the queued messages from talkto to user
                    lock.acquire()
                    with open("histories.json", "r+") as f:
                        data = json.load(f)
                        # from talkto to user

                        if talkto in data.keys():

                            if username in data[talkto].keys():
                                # maybe data[talkto][user] will return a key not find error
                                messages = data[talkto][username]
                                if not len(messages)==0:
                                    client.send("NOTEMPTY".encode('ascii'))
                                    tosend = pickle.dumps(messages)
                                    client.send(tosend)
                                
                                # after send all the queued messages, clear the history
                                    print("------clear the message--------")
                                    #data[talkto][user] = []lock.acquire()
                                    #lock.acquire()
                                    with open("histories.json", "r") as f2:
                                        data = json.load(f2)
                                        data[talkto][username] = []
                                    with open("histories.json", 'w') as f2:
                                        json.dump(data, f2, indent=4)
                                    #lock.release()
                                    print("------finish clean--------")
    
                                else:
                                    client.send("EMPTY".encode('ascii'))
                            else:
                                    client.send("EMPTY".encode('ascii'))
                        else:
                            client.send("EMPTY".encode('ascii'))
                    lock.release()
                # If the request is a "STARTCHAT" request, it checks if the requested user is currently logged in. 
                # If the user is logged in, it starts a chat session. 
                # Otherwise, it queues the user's messages to be sent to the requested user when they next log in.
                elif operation == "STARTCHAT":
                    # check if the person trying to talkto is online
                    # if online, start chat
                    # if offline, queue the user's messages
                    '''
                    if talkto in logins:
                        client.send("CHATNOW".encode('ascii'))
                    else:
                        client.send("CHATLATER".encode('ascii'))
                    '''
                    self.message_receiver(client, talkto, username)
                    #if the user want to start over, the chattiing part is wrapped in the receive.
                    print("server chat breaking")

                elif operation.startswith("BREAK"):
                    if client:
                        client.close()
                    if server:
                        server.close()
                    break

                elif operation.startswith("CHATTT"):
                    '''
                    this happens if the user reconncts to the server
                    '''
                    # reconnection and 
                    last_info = self.log[-1]['Content']
                    # assuming the last_info is not a set
                    user_talk_to = last_info.split("~")[1]
                    user_itself = last_info.split("~")[2]
                    l1 = len(user_talk_to)
                    l2 = len(user_itself)
                    talkto = user_talk_to
                    username = user_itself 
                    self.message_receiver(client, user_talk_to, user_itself)
                else:
                    print(" line 888 operation is not supportted")
                    if client:
                        client.close()
                    break

        except Exception as e:
            print("client error")
            print(e)
            if client:
                client.close()

    def broadcast(self, msg, name):  # prefix is for name identification.
        """Broadcasts a message to all the servers."""
        message = {'Command': 'Broadcast', 'msg': msg, 'name': name}
        CONFIG = json.load(open("config.json"))
        self.server_port = CONFIG['server_port']
        server_on_list = CONFIG['server_on']
        for server_id in self.server_port:
            if server_id != self.server_id and server_id in server_on_list:
                self.sendMessage(server_id, message)

    def broadcast_client(self, msg, prefix=""):
        print("do nothing")
        '''
        for sock in self.clients_con:
            sock.send(bytes(prefix + msg, "utf8"))
        '''



    def receive(self, client, addr, LOGIN_LIMIT = 5, login_times = 0):
        """
        This function has four parameters:
        client: the client object that is connected to the server
        addr: the address of the client
        LOGIN_LIMIT: an integer that limits the number of login attempts a user can make before being blocked from the server
        login_times: an integer that keeps track of the number of login attempts made by a user

        """
        
        # connect with the client
        # client, addr = server.accept()
        print(f"Connected with {str(addr)}")
        username = ""
        talkto = ""
        try:
            while True:
                
                operation = client.recv(1024).decode('ascii')
                print("operation is ", operation)
                self.rec_client(operation)
                # If the request is a "LOGIN" request, it checks if the user has exceeded the login limit. 
                # If the user has exceeded the limit, it sends a "FAIL" message to the client and closes the connection. 
                # Otherwise, it prompts the user to enter their username and password, checks if the username and password are correct, and either logs the user in or rejects the login attempt.
                if operation == "LOGIN":
                    self.login_times += 1
                    print("logging...")
                    if self.login_times > self.LOGIN_LIMIT:
                        client.send("FAIL".encode('ascii'))
                        client.close()

                    # check whether username and password match
                    # ask the client for the username
                    client.send("USERNAME".encode('ascii'))
                    username = client.recv(1024).decode('ascii')

                    client.send('PASSWORD'.encode('ascii'))
                    password = client.recv(1024).decode('ascii')

                    # check if password matches with user name
                    with open('accounts.json') as f:
                        data = json.load(f)
                    if username not in data.keys():
                        print("No such user")
                        client.send("NOUSER".encode('ascii'))
                    elif password != data[username]["password"]:
                        print("Login rejected!")
                        client.send("REJECT".encode('ascii'))
                        #continue # returns the control to the beginning of the while loop
                    else:
                        print("Successfully logged in! as ", username)
                        client.send("ACCEPT".encode('ascii'))
                        self.clients[username] = client
                        print("add " + username + " to logins")

                
                # If the request is a "SIGNUP" request, it checks if the requested username is already taken. 
                # If the username is available, it prompts the user to enter a password, generates a unique ID for the new account, and stores the account information in a JSON file.
                elif operation.startswith("SIGNUP"):
                    # check whether the username already exists
                    username = operation.split(" ")[1]
                    with open('accounts.json') as f:
                        data = json.load(f)
                    if username in data:
                        client.send('DUPNAME'.encode('ascii'))
                    else:
                        client.send('NONDUPNAME'.encode('ascii'))
                        password = client.recv(1024).decode('ascii')
                        # generate a unique uuid for the new account
                        id = uuid.uuid1()
                        # store the new created account into the json file
                        with open("accounts.json", "w") as f:
                            data[username] = {"password": password}
                            json.dump(data, f, indent=4)

                # If the request is a "LIST" request, it searches for usernames that match a given pattern in a JSON file containing account information. 
                # If there are matches, it sends a list of the matching usernames to the client.
                elif operation.startswith("LIST"):
                    pattern = operation.split(" ")[1]
                    with open('accounts.json') as f:
                            data = json.load(f)
                    if pattern == "ALL":
                        keys = list(data.keys())
                    else:
                        keys = []
                        for k in list(data.keys()):
                            if fnmatch.fnmatch(k, pattern):
                                keys.append(k)
                    if len(keys) == 0:
                        client.send("NOMATCHED".encode('ascii'))
                    else:
                        client.send("MATCHED".encode('ascii'))
                        next_line = client.recv(1024).decode('ascii')
                        if next_line == "SENDMATCHED":
                            lists = pickle.dumps(keys)
                            client.send(lists)   

                # If the request is a "TALKTO" request, it checks if the requested username is valid. 
                # If the username is valid, it adds the user to a set of logged-in users.
                elif operation.startswith("TALKTO"):
                    talkto = operation.split(" ")[1]
                    # check if the talkto username is valid
                    with open('accounts.json') as f:
                        data = json.load(f)
                    if talkto in data:
                        print(talkto + " is a valid user")
                        client.send("VALTALKTO".encode('ascii'))
                        self.logins.add(username)
                        self.rec_logins()
                    else:
                        client.send("INVALTALKTO".encode('ascii'))
                        # lists = pickle.dumps(list(data.keys()))
                        # client.send(lists)
                # If the request is a "STARTHIST" request, it sends any queued messages from a previous chat session with a user to the current user.
                elif operation == "STARTHIST":
                    # send the queued messages from talkto to user
                    lock.acquire()
                    with open("histories.json", "r+") as f:
                        data = json.load(f)
                        # from talkto to user

                        if talkto in data.keys():

                            if username in data[talkto].keys():
                                # maybe data[talkto][user] will return a key not find error
                                messages = data[talkto][username]
                                if not len(messages)==0:
                                    client.send("NOTEMPTY".encode('ascii'))
                                    tosend = pickle.dumps(messages)
                                    client.send(tosend)
                                
                                # after send all the queued messages, clear the history
                                    print("------clear the message--------")
                                    #data[talkto][user] = []lock.acquire()
                                    #lock.acquire()
                                    with open("histories.json", "r") as f2:
                                        data = json.load(f2)
                                        data[talkto][username] = []
                                    with open("histories.json", 'w') as f2:
                                        json.dump(data, f2, indent=4)
                                    #lock.release()
                                    print("------finish clean--------")
    
                                else:
                                    client.send("EMPTY".encode('ascii'))
                            else:
                                    client.send("EMPTY".encode('ascii'))
                        else:
                            client.send("EMPTY".encode('ascii'))
                    lock.release()
                # If the request is a "STARTCHAT" request, it checks if the requested user is currently logged in. 
                # If the user is logged in, it starts a chat session. 
                # Otherwise, it queues the user's messages to be sent to the requested user when they next log in.
                elif operation == "STARTCHAT":
                    # check if the person trying to talkto is online
                    # if online, start chat
                    # if offline, queue the user's messages
                    '''
                    if talkto in logins:
                        client.send("CHATNOW".encode('ascii'))
                    else:
                        client.send("CHATLATER".encode('ascii'))
                    '''
                    self.message_receiver(client, talkto, username)
                    #if the user want to start over, the chattiing part is wrapped in the receive.
                    print("server chat breaking")

                elif operation.startswith("BREAK"):
                    if client:
                        client.close()
                    if server:
                        server.close()
                    break
                else:
                    print("line 1090 operation is not supportted")
                    if client:
                        client.close()
                    break

        except Exception as e:
            print("client error")
            print(e)
            if client:
                client.close()

    def get_last_set_value(self, input_list):
        print("line 1138")
        for item in reversed(input_list):
            if isinstance(item['Content'], set):
                return item['Content']
        return None

    def message_receiver(self, client, talkto, user):
        """
        Receives message from the client side. 
        """
        # A function handling the chatting part
        print("in the message receiver")
        print("talk to is ", talkto, " user is ", user)
        try:
            while True:
                # update the self.logins with the log
                res = self.get_last_set_value(self.log)
                if res is not None:
                    self.logins = res
                if talkto in self.logins:
                    client.send("CHATNOW".encode('ascii'))
                    recv_message = client.recv(1024).decode('ascii')
                    print("line 1115 ", recv_message)
                    self.rec_client(recv_message)
                    user_talk_to = recv_message.split("~")[1]
                    user_itself = recv_message.split("~")[2]
                    l1 = len(user_talk_to)
                    l2 = len(user_itself)
                    user_message = recv_message[7 + l1 + l2 + 2:]
                    if user_message == "\exit":
                        #user log out 
                        self.clients.pop(user_itself)
                        self.logins.remove(user_itself)
                        self.rec_logins()
                        # TODO Do we need to close the client when exit???

                    elif user_message == "\switch":
                        client.send("SWITCH".encode('ascii'))
                        return

                    elif user_message == "\delete":
                        print(user_itself + " deleted its account")
                        # delete from json
                        lock.acquire()
                        with open("accounts.json") as f:
                            data = json.load(f)
                            data.pop(user_itself)
                        with open("accounts.json", "w") as f:
                            print("deleting from json")
                            json.dump(data, f, indent=4)
                        lock.release()
                        # tell the talkto
                        print("tell " + user_talk_to + " that " +user_itself + " is deleted")
                        self.clients[user_talk_to].send("TALKTODELETED".encode('ascii'))
                        # confirm with the user that he's deleted
                        client.send("CONFIRMDELETED".encode("ascii"))
                        #user log out 
                        self.clients.pop(user_itself)
                        self.logins.remove(user_itself)
                        self.rec_logins()
                        raise delete_account_exception("deleted")

                    else:
                        self.clients[user_talk_to].send(("CHATNOW" + user_itself + " : "+user_message).encode('ascii')) 
                else:
                    client.send("CHATLATER".encode('ascii'))
                    recv_message = client.recv(1024).decode('ascii')
                    print("line 1152 ", recv_message)
                    self.rec_client(recv_message)
                    user_talk_to = recv_message.split("~")[1]
                    user_itself = recv_message.split("~")[2]
                    l1 = len(user_talk_to)
                    l2 = len(user_itself)
                    user_message = recv_message[7 + l1 + l2 + 2:]
                    if user_message == "\exit":
                        #user log out 
                        self.clients.pop(user_itself)
                        self.logins.remove(user_itself)
                        self.rec_logins()
                        client.send("EXIT".encode('ascii'))
                        return

                    if user_message == "\switch":
                        client.send("SWITCH".encode('ascii'))
                        return

                    if user_message == "\delete":
                        print(user_itself + " deleted its account")
                        # delete from json
                        lock.acquire()
                        with open("accounts.json") as f:
                            data = json.load(f)
                            data.pop(user_itself)
                        with open("accounts.json", "w") as f:
                            print("deleting from json")
                            json.dump(data, f, indent=4)
                        lock.release()
                        # tell the talkto
                        print("tell " + user_talk_to + " that " +user_itself + " is deleted")
                        # This line will cause key not find error if clients doesn't have the key 'user_talk_to'
                        self.clients[user_talk_to].send("TALKTODELETED".encode('ascii'))
                        # confirm with the user that he's deleted
                        client.send("CONFIRMDELETED".encode("ascii"))
                        #user log out 
                        self.clients.pop(user_itself)
                        self.logins.remove(user_itself)
                        self.rec_logins()
                        raise delete_account_exception("deleted")


                    #write to the json file 
                    print("the user is not logged in")
                    lock.acquire()
                    with open("histories.json", "r+") as f:
                        data = json.load(f)
                        # from user to talkto
                        print("now writing to json", user_message)
                        try:
                            old_list = data[user_itself][user_talk_to]
                            old_list.append(user_message)
                            data[user_itself][user_talk_to] = old_list
                        except:
                            # data[user_itself] exists but data[user_itself][user_talk_to] is not 
                            print("create a new key of dictionary")
                            old_list = []
                            old_list.append(user_message)
                            try :
                                print("in try data[user_itself][user_talk_to] = old_list")
                                data[user_itself][user_talk_to] = old_list
                            except:
                                # data[user_itself] not exists
                                print("create a new dictionary of dictionary")
                                data[user_itself] = {}
                                old_list = []
                                old_list.append(user_message)
                                data[user_itself][user_talk_to] = old_list

                    with open("histories.json", 'w') as f:
                        json.dump(data, f, indent=4)
                    lock.release()

        except delete_account_exception:
            return delete_account_exception("deleted")

        except Exception as e:
            #This user try to logout or encounter connection error
            print("exception raised in message_receiver")
            print(e)
            self.clients.pop(user)
            self.logins.remove(user)
            self.rec_logins()
            if client:
                client.close()

        return
                        
    def start2(self):
        # keep a set of already logged in users
        self.logins = set()
        # keep a number of logged in times, exceeding this limit will cause break of the connection
        self.LOGIN_LIMIT = 5

        self.login_times = 0

        # talkto = ''
        self.clients = {} 
        print('Listening...')
        
        try:
            while True:
                print("-------------------server start------------------------")
                client, addr = self.server.accept()
                self.clients_con.append(client)
                print("%s:%s has connected." % addr)
                client.send(bytes("Welcome! You are at server %s. Type your username and press enter to continue."%self.server_id, "utf8"))
                self.addresses[client] = addr
                #print(f"Connected with {str(addr)}")    
                t = threading.Thread(target=self.receive, args=(client, addr))
                t.start()

            
            #stop the server from break
            #server.close()     
        except Exception as e:
            print('Error Occurred: ', e)
            print("stop the server")
            server.close()
'''
if __name__ == '__main__':
    start()
'''
if __name__ == "__main__":
    CONFIG = json.load(open("config.json"))
    server_on_list = CONFIG['server_on']
    all_server_id = sorted(CONFIG['server_port'].keys())
    for i in all_server_id:
        if i not in server_on_list:
            server_id = i
            break
    #print('KeyboardInterrupt')
    try:
        CONFIG['server_on'].append(server_id)
    except:
        print('no more place for another server!')
        sys.exit(1)

    try:

        server = Server(server_id, CONFIG)
        print("Connect to server ", server_id)
        server.start()

    except Exception as e:
        print('Connect to server is not done')
        print(e)
        server_id = server_id
        CONFIG = json.load(open("config.json"))
        CONFIG['server_on'].remove(server_id)
        json.dump(CONFIG, open('config.json', 'w'))
        # os.system('python3 state_ini.py 5')
