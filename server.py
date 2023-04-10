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


def receive(client, addr, LOGIN_LIMIT = 5, login_times = 0):
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

            # If the request is a "LOGIN" request, it checks if the user has exceeded the login limit. 
            # If the user has exceeded the limit, it sends a "FAIL" message to the client and closes the connection. 
            # Otherwise, it prompts the user to enter their username and password, checks if the username and password are correct, and either logs the user in or rejects the login attempt.
            if operation == "LOGIN":
                login_times += 1
                print("logging...")
                if login_times > LOGIN_LIMIT:
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
                    clients[username] = client
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
                    logins.add(username)
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
                message_receiver(client, talkto, username)
                #if the user want to start over, the chattiing part is wrapped in the receive.
                print("server chat breaking")

            elif operation.startswith("BREAK"):
                if client:
                    client.close()
                if server:
                    server.close()
                break
            else:
                print("operation is not supportted")
                if client:
                    client.close()
                break

    except Exception as e:
        print("client error")
        print(e)
        if client:
            client.close()

class delete_account_exception(Exception):
    def __init__(self, message):
        print(message)

def message_receiver(client, talkto, user):
    """
    Receives message from the client side. 
    """
    # A function handling the chatting part
    print("in the message receiver")
    print("talk to is ", talkto, " user is ", user)
    try:
        while True:
            if talkto in logins:
                client.send("CHATNOW".encode('ascii'))
                recv_message = client.recv(1024).decode('ascii')
                user_talk_to = recv_message.split("~")[1]
                user_itself = recv_message.split("~")[2]
                l1 = len(user_talk_to)
                l2 = len(user_itself)
                user_message = recv_message[7 + l1 + l2 + 2:]
                if user_message == "\exit":
                    #user log out 
                    clients.pop(user_itself)
                    logins.remove(user_itself)
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
                    clients[user_talk_to].send("TALKTODELETED".encode('ascii'))
                    # confirm with the user that he's deleted
                    client.send("CONFIRMDELETED".encode("ascii"))
                    #user log out 
                    clients.pop(user_itself)
                    logins.remove(user_itself)
                    raise delete_account_exception("deleted")

                else:
                    clients[user_talk_to].send(("CHATNOW" + user_itself + " : "+user_message).encode('ascii')) 
            else:
                client.send("CHATLATER".encode('ascii'))
                recv_message = client.recv(1024).decode('ascii')
                user_talk_to = recv_message.split("~")[1]
                user_itself = recv_message.split("~")[2]
                l1 = len(user_talk_to)
                l2 = len(user_itself)
                user_message = recv_message[7 + l1 + l2 + 2:]
                if user_message == "\exit":
                    #user log out 
                    clients.pop(user_itself)
                    logins.remove(user_itself)
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
                    clients[user_talk_to].send("TALKTODELETED".encode('ascii'))
                    # confirm with the user that he's deleted
                    client.send("CONFIRMDELETED".encode("ascii"))
                    #user log out 
                    clients.pop(user_itself)
                    logins.remove(user_itself)
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
        clients.pop(user)
        logins.remove(user)
        if client:
            client.close()

    return
                    

if __name__ == '__main__':
    host = "127.0.0.1"
    port = 5555
    # keep a set of already logged in users
    logins = set()
    # keep a number of logged in times, exceeding this limit will cause break of the connection
    LOGIN_LIMIT = 5

    login_times = 0

    # talkto = ''
    clients = {} 
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    os.chdir(sys.path[0])
    server.bind((host, port))
    server.listen(10)
    print('Listening...')
    try:
        while True:
            print("-------------------server start------------------------")
            client, addr = server.accept()
            #print(f"Connected with {str(addr)}")    
            t = threading.Thread(target=receive, args=(client, addr))
            t.start()

        
        #stop the server from break
        #server.close()     
    except Exception as e:
        print('Error Occurred: ', e)
        print("stop the server")
        server.close()
