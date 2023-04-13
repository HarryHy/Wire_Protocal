import os
import threading
from socket import *
from threading import Thread
import tkinter
import json
import sys
import time
from queue import Queue
import argparse
import pickle
lock = threading.Lock()
clear = os.system("cls||clear")
# global variables
stop = False
username = "example"
password = "123"
from datetime import datetime
now = datetime.now()

current_time = now.strftime("%H:%M:%S")

# def check_dupname():
class restart_conversation_exception(Exception):
    def __init__(self, message):
        print(message)

class delete_account_exception(Exception):
    def __init__(self, message):
        print(message)
global receive_begin 
receive_begin = True

class Client():
    def __init__(self, server_id):  # removed tk as third argument
        CONFIG = json.load(open("config.json"))
        PORT = CONFIG['server_port'][server_id]['port']


        self.received_messages = []
        self.talkto = "user2"
        self.omit = False

        # ----Now comes the sockets part----
        HOST = 'localhost'
        self.BUFSIZ = 1024
        ADDR = (HOST, PORT)
        print(ADDR)
        self.client = socket(AF_INET, SOCK_STREAM)

        self.client.connect(ADDR)
        print(self.client)
        self.message_queue = Queue()

        '''
        receive_thread = Thread(target=self.receive)
        receive_thread.start()
        '''
        '''
        global write_thread
        write_thread = Thread(target=self.send)
        write_thread.start()
            '''
        self.reconnect_thread = Thread(target=self.reconnect)
        self.reconnect_thread.start()

        self.receive_thread = Thread(target=self.chat)
        self.receive_thread.start()

        

    def reconnect(self):

        #print(";ine 75 ", self.client)
        #if not self.client:
        print("reconnect")
        start = time.time()
# CONFIG = json.load(open("config.json"))

        server_id = 0

        end = time.time()
        while end - start < 10:
            end = time.time()
            print("server_id, ", server_id)
            try:
                with open("config.json", "r") as f:
                    CONFIG = json.load(f)
                    server_id = server_id % 5
                    self.client = socket(AF_INET, SOCK_STREAM)
                    self.client.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                    PORT = CONFIG['server_port'][str(server_id)]['port']

                    HOST = 'localhost'
                    ADDR = (HOST, PORT)
                    self.client.connect(ADDR)
                    break
            except Exception as e:
                # Same exception handling
                print("server_id, ", server_id, " error is ", e)
                server_id += 1
                server_id = server_id % 5
                self.client.close()
                #f.close()
                continue
        #print("self.client is --------------------------------- ", self.client)
        #print(self.client)

    def start_receive_thread(self):
        if self.receive_thread is not None and self.receive_thread.is_alive():
            print("Receive thread is already running")
            return

        self.receive_thread = Thread(target=self.chat)
        self.receive_thread.start()

    def chat(self):
        self.choose_operations()
        self.start_conversation()
    '''    
    def reconnect(self):

        # client cannot connect to the server, try to connect to the leader
        start = time.time()
        # CONFIG = json.load(open("config.json"))
        server_id = 0

        end = time.time()
        while end - start < 60:
            end = time.time()
            print("server_id, ", server_id)
            try:
                with open("config.json", "r") as f:
                    CONFIG = json.load(f)
                    server_id = server_id % 5
                    self.client_socket = socket(AF_INET, SOCK_STREAM)
                    self.client_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                    PORT = CONFIG['server_port'][str(server_id)]['port']

                    HOST = 'localhost'
                    ADDR = (HOST, PORT)
                    self.client_socket.connect(ADDR)
                    break
            except Exception as e:
                # Same exception handling
                print("server_id, ", server_id, " error is ", e)
                server_id += 1
                server_id = server_id % 5
                self.client_socket.close()
                f.close()
                continue
        while not self.message_queue.empty():
            msg = self.message_queue.get()
            # self.msg_list.insert(tkinter.END, msg)
            print(msg)
        # self.display_messages()
        self.received_messages = []
        # self.top.update_idletasks()
        # self.top.update()

        print("outside reconnect")
        self.start_receive_thread()
        # print("reconnection not success")
        return
    '''

    def receive2(self):
        """Handles receiving of messages."""
        print("receive")
        while True:
            try:
                msg = self.client_socket.recv(self.BUFSIZ).decode("utf8")
                print("line 132 ", msg)
                self.received_messages.append(msg)
                if "SERVERINFO" in msg:
                    if msg.find("SERVERINFO") == 0:
                        # self.msg_list1.insert(tkinter.END, msg)
                        print(msg)
                    else:
                        msg1 = msg[0:msg.find("SERVERINFO")]
                        msg2 = msg[msg.find("SERVERINFO"):]
                        # self.msg_list.insert(tkinter.END, msg1)
                        print(msg1)
                        print(msg2)
                        # self.msg_list1.insert(tkinter.END, msg2)
                else:
                    if msg == "INVALID":
                        # self.msg_list.insert(
                        # tkinter.END, "Invalid command, please input again")
                        print("Invalid command, please input again")
                    ######################### LOGIN ############################
                    elif msg == "NOUSER":
                        # when no such user exists, create a new account
                        # self.msg_list.insert(
                        # tkinter.END, "No such user exists, need to create a new account, please input new user and new password\n (SIGNUP “username+password”)")
                        print(
                            "No such user exists, need to create a new account, please input new user and new password\n (SIGNUP~'username+password')")
                        # user should type SIGNUP “username+password”
                    elif msg == "DUPNAME":
                        # self.msg_list.insert(
                        #    tkinter.END, "Username already exists, please input new user and new password\n (SIGNUP “username+password”)")
                        print(
                            "Username already exists, please input new user and new password\n (SIGNUP~'username+password')")
                        # user should type SIGNUP “username+password”
                    elif msg == "WRONGPASS":
                        # self.msg_list.insert(
                        #    tkinter.END, "Wrong password, please reenter login username and password\n (LOGIN “username+password”)")
                        print(
                            "Wrong password, please re-enter login username and password\n (LOGIN~'username+password')")
                        # user should type LOGIN “username+password” in send()
                    elif msg.startswith("LOGINSUCCESS"):
                        welcome_msg = msg[12:]
                        # self.login = True
                        # self.msg_list.insert(
                        #    tkinter.END, welcome_msg + " Can start talking or listing accounts\n (LIST 'pattern' or TALKTO 'username+message')")
                        print(
                            welcome_msg + " \n Can start talking or listing accounts\n (LIST 'pattern' or TALKTO 'username+message')")
                        # user should type LIST "pattern" or TALKTO “username”

                    ######################### LIST ACCOUNTS ############################
                    elif msg == "NOMATCHED":
                        # self.msg_list.insert(
                        #    tkinter.END, "No matched account found")
                        print("No matched account found")
                    elif msg.startswith("MATCHED"):
                        # self.msg_list.insert(
                        #    tkinter.END, "Matched accounts found: " + msg[7:])
                        print("Matched accounts found: " + msg[7:])

                    ######################### TALKTO ############################
                    # elif msg.startswith("VALTALKTO"):
                    #     self.msg_list.insert(tkinter.END, "You are now talking to " + msg[9:])
                    elif msg == "INVALTALKTO":
                        # self.msg_list.insert(
                        #    tkinter.END, f"The username {msg[11:]} doesn't exist, please try talking to another person\n (TALKTO “username+message”)")
                        # user should tyle TALKTO “username+message” in send()
                        print(
                            "The username doesn't exist, please try talking to another person\n (TALKTO 'username+message')")
                    elif msg.startswith("OFFLINE"):
                        print("offline ", self.omit)
                        if self.omit is False:
                            # self.msg_list.insert(
                            #    tkinter.END, f"The user {msg[7:]} you are talking to is offline, messages you typed above will be queued")
                            print(
                                f"The user {msg[7:]} you are talking to is offline, messages you typed above will be queued")
                    elif msg.startswith("QUEUED"):
                        # self.msg_list.insert(tkinter.END, msg[6:])
                        print(msg[6:])
                        
                    else:
                        # self.msg_list.insert(tkinter.END, msg)
                        print(msg)
                # self.top.update_idletasks()
                # self.top.update()
            except Exception as e:  # Possibly client has left the chat.
                print("OSERROR, ", e)
                break
        self.message_queue.put("Disconnected. Attempting to reconnect...")
        # print("Disconnected. Attempting to reconnect...")
        # self.top.update_idletasks()
        # self.top.update()

        try:
            print("start to reconnect")
            self.reconnect()
        except Exception as e:
            print("Error ", e)
            print("cannot reconnect")
            return
    '''
    def on_closing(self):
        """This function is to be called when the window is closed."""
        self.my_msg.set("{quit}")
        self.send()
        self.client_socket.close()
        self.top.quit()
    '''

    '''def update_gui(self):
        try:
            if self.message_queue.empty() is False:
                msg = self.message_queue.get_nowait()
                self.message_queue
            else:
                self.msg_list.insert(tkinter.END, msg)

            self.top.after(100, self.update_gui)
        except:
            pass
    '''

    def switch_user(self, user):
        self.talkto = user
        self.omit = False

    def send(self, event=None):  # event is passed by binders.
        """Handles sending of messages."""
        # self.my_msg.set("")  # Clears input field.
        try:
            # TODO: Add a while loop here?
            chat_break = False
            while True:
                if chat_break:
                    break
                msg = input("Enter operation: ")
                if msg.startswith("SWITCH"):
                    username = msg[7:]
                    print(username)
                    chat_break = True
                    self.switch_user(username)
                elif msg.startswith("OMIT"):
                    chat_break = True
                    self.omit = True
                elif msg.startswith("LIST"):
                    chat_break = True
                    self.client_socket.send(bytes(msg, "utf8"))
                elif msg.startswith("LOGIN"):
                    chat_break = True
                    self.client_socket.send(bytes(msg, "utf8"))
                elif msg.startswith("SIGNUP"):
                    chat_break = True
                    self.client_socket.send(bytes(msg, "utf8"))
                elif msg.startswith("DELETE"):
                    chat_break = True
                    self.client_socket.send(bytes(msg, "utf8"))
                elif msg.startswith("RECONNECT"):
                    self.start_receive_thread()
                else:
                    msg = "TALKTO~" + self.talkto + "+" + msg
                    self.client_socket.send(bytes(msg, "utf8"))
                print("message is ", msg)
                # self.client_socket.send(bytes(msg, "utf8"))
        except:
            self.client_socket.close()
            # self.top.quit()
        if msg == "{quit}":
            self.client_socket.close()
            # self.top.quit()

    '''def on_closing(self, event=None):
        """This function is to be called when the window is closed."""
        self.my_msg.set("{quit}")
        self.send()
        self.client_socket.close()
        self.top.quit()
        '''




    def login(self):
        """
        Send LOGIN message to the server and prompt user to input username and password
        """
        try:
            # Send the 'LOGIN' command to the server
            self.client.send('LOGIN'.encode('ascii'))
            # Clear the console
            os.system("cls||clear")
            # Prompt the user to enter their username and password
            global username
            global password
            username = input("Enter the username: ")
            password = input("Enter the password: ")
            # Set the 'stop' flag to False, indicating that the client is still running
            global stop
            stop = False
        except Exception as e:
            # If an error occurs, print an error message and close the connection to the server
            print('Error Occurred in login: ', e)
            if self.client:
                self.client.close()
    
    


    def signup(self):  
        """
        Handle the process of creating a new account.
        It prompts the user to create a unique username, sends it to the server for duplicate check, and then asks for a password. 
        If the username already exists, it prompts the user to choose another one. 
        """
        os.system('cls||clear')
        # check if username is unique
        global username
        global password
        while True:
            try:
                username = input("Create your username: ")
                # send the username to server for duplicate check
                self.client.send(('SIGNUP '+username).encode('ascii'))
                dup_message = self.client.recv(1024).decode('ascii')
                if dup_message == "DUPNAME":
                    print("Username already exists! Change to another one.")
                elif dup_message == "NONDUPNAME":
                    password = input("Create your password: ")
                    self.client.send(password.encode('ascii'))
                    break

            except Exception as e:
                print('Error Occurred: ', e)

        

    
    def listAccounts(self):
        """
        Allows the user to list all or a subset of the accounts by text wildcard.
        The function prompts the user to choose one of two options: either to list all accounts, or to list accounts by a specific search pattern (wildcard).
        After receiving a response from the server, the function checks if there are any matched accounts or not. 
        If no accounts are matched, it prints a message indicating that no matched account was found. 
        If there are matched accounts, the function sends a message to the server to indicate that it is ready to receive the matched accounts. 
        Then, the function receives the matched accounts as a pickled object and prints them to the console.
        """
        os.system('cls||clear')
        # list all or a subset of the accounts by text wildcard
        while True:
            # Ask the user whether to list all accounts or by wildcard
            option = input("(1)List all \n(2)List by wildcard\n")
            if option == "1":
                self.client.send('LIST ALL'.encode('ascii'))
                break
            elif option == "2":
                # Ask the user to input the search pattern
                pattern = input("Input your search pattern: ")
                self.client.send(('LIST '+pattern).encode('ascii'))
                break
            else: 
                print("Invalid option, choose again")
        
        response = self.client.recv(1024).decode('ascii')
        if response == "NOMATCHED":
            print("No matched account found")
        elif response == "MATCHED":
            self.client.send("SENDMATCHED".encode('ascii'))
            # Receive list of matched accounts from server
            list_bytes = self.client.recv(4096)
            list_accounts = pickle.loads(list_bytes)
            # Print each account in the list
            for a in list_accounts:
                print(a)
            

    def receive(self):
        """
        Receive messages from the server.
        """
        try:
            while True:
                global stop
                if stop: break
                print("in the checking username step")

                message = self.client.recv(1024).decode('ascii')
                print("message ", message)
                if message == "USERNAME":
                    self.client.send(username.encode('ascii'))
                    next_message = self.client.recv(1024).decode('ascii')
                    if next_message == 'PASSWORD':
                        self.client.send(password.encode('ascii'))
                        check_state = self.client.recv(1024).decode('ascii')
                        if check_state == "REJECT":
                            print("Wrong password! Try again")
                            stop = True
                        elif check_state == "NOUSER":
                            print("No such user")
                            stop = True
                        else:
                            print("Successfully logged in as ", username)
                            return
                elif message == "FAIL":
                    print("You've reach the attemp limit, connection failed.")
                else: 
                    print(" the message is not on the list")
                    print(message)
        except Exception as e:
            print('Error Occurred: ', e)
            if self.client:
                self.client.close()
            print("start to reconnect")
            self.reconnect()
        self.choose_operations()

    def choose_operations(self):
        """
        Displays a menu of options to the user (sign in, sign up, or list existing accounts), waits for the user to make a choice, and then calls the appropriate function based on the user's choice. 
        """
        try:
            while True:
                option = input("(1)Sign in\n(2)Sign up\n(3)List existing accounts\n")
                if option == "1":
                    self.login()
                    break
                elif option == "2":
                    self.signup()
                elif option == "3":
                    self.listAccounts()
                else: 
                    print("Invalid option, choose again")
            self.receive()
        except Exception as e:
            print("line 496 ", e)
            if self.client:
                self.client.close()
            print("start to reconnect")
            self.reconnect()

    def choose_talkto(self):
        """
        Prompts the user to choose another user to talk to.
        It sends a "TALKTO" message to the server along with the specified username. 
        The server responds with a "VALTALKTO" message if the specified username is valid, indicating that the conversation can start. 
        If the specified username is not valid, the server sends an "INVALTALKTO" message indicating that the user doesn't exist and the user is prompted to try another username. 
        The function continues to loop until a valid username is entered and the server sends the "VALTALKTO" message.
        """
        print("please choose who to talk to")
        choose_talk_to_stop = False
        while True:
            if choose_talk_to_stop: 
                break
            global talkto
            talkto = input("Who do you want to talk to? (specify the username) ")
            self.client.send(("TALKTO "+talkto).encode('ascii'))
            next_message = self.client.recv(1024).decode('ascii')
            if next_message == "VALTALKTO":
                print("next_message is " + next_message)
                print("Start your conversation with "+talkto + "!")
                choose_talk_to_stop = True
                break
            elif next_message == "INVALTALKTO":
                print("The username you were trying to talk to doesn't exist, please try another one.")
            else: 
                print("invalid response of choose_talkto" + next_message)


    def start_conversation(self):
        """
        Starts a conversation with another user, by first receiving conversation history. 
        If there are any queued messages, it receives them and displays them. 
        Then, it sends a message to the server to start the chat, and starts two threads to handle writing and receiving messages.
        """
        try:
            self.choose_talkto()
            self.client.send('STARTHIST'.encode('ascii'))
            print("finish client.send('STARTHIST'.encode('ascii'))")
            # receive all the queued messages
            flag = self.client.recv(1024).decode('ascii')
            print("flag is ", flag)
            if flag != "EMPTY":
                list_bytes = self.client.recv(4096)
                #print("list_bytes is ", list_bytes)
                list_messages = pickle.loads(list_bytes)
                for m in list_messages:
                    print(talkto + " : " + m)
            print("--------------start to chat-----------------")
            # after receive the history, start to chat
            self.client.send('STARTCHAT'.encode('ascii'))
        except Exception as e:
            print("line 559 ", e)
            '''
            if self.client:
                self.client.close()
            '''
            print("start to reconnect")
            self.reconnect()
            #self.start_conversation()
        
        print("start to talk")
        self.talking()

    def talking(self):
        try:
            global write_thread
            write_thread = threading.Thread(target=self.write_messages)
            write_thread.start()
            global receive_thread
            receive_thread = threading.Thread(target=self.receive_messages)
            receive_thread.start()
        
            
        except restart_conversation_exception:
            print('actually returned to start_conversation')
            write_thread.join()
            receive_thread.join()
            print("There are these number of threads running after restart" + threading.active_count())
            self.start_conversation()

        except Exception as e:
            print("line 586")
            print('Error Occurred: ', e)
            write_thread.join()
            receive_thread.join()
            if self.client:
                self.client.close()
            print("line 592 ", e)
            print("start to reconnect")
            self.reconnect()
            self.talking()

    def write_messages(self):
        """
        Write messages from the client to the server.
        Handles some special cases like exit, switch, and delete.
        """
        try:

            chat_break = False
            while True:

                if chat_break:
                    break
                
                input_message = input()
                if input_message == "\exit":
                    chat_break = True
                    self.client.send(('EXITTT~' + talkto +"~" + username + "~"+ input_message).encode('ascii'))
                    print("end of exit")
                    return
                elif input_message == "\switch":
                    chat_break = True
                    self.client.send(('SWITCH~' + talkto +"~" + username + "~"+ input_message).encode('ascii'))
                    print("end of switch")
                    raise restart_conversation_exception("restart")
                elif input_message == "\delete":
                    # delete self account
                    self.client.send(('DELETE~' + talkto +"~" + username + "~"+ input_message).encode('ascii'))
                    # raise delete_account_exception("")
                    print("end of delete")
                    print("You are forced to log out")
                    return
                else:
                    print(username + " : " + input_message)
                    self.client.send(('CHATTT~' + talkto +"~" + username + "~"+ input_message).encode('ascii'))
        except restart_conversation_exception:

            
            self.start_conversation()
        
        except Exception as e:
            #raise Exception("line 638 reconnection")
            write_thread.join()
            if self.client:
                self.client.close()
            print("line 642 ", e)
            print("start to reconnect")
            self.reconnect()
            #global write_thread
            write_thread = threading.Thread(target=self.write_messages)
            write_thread.start()
            #self.talking()
            


    def receive_messages(self):
        """
        Receives messages from the server and processes them based on their contents
        """
        online_flag = False
        omit_message = False
        while True:
            try:
                message = self.client.recv(1024).decode('ascii')
                if message.startswith("CHATNOW"):
                    if online_flag == False:
                        online_flag = True
                        print("this user is online now")
                        omit_message = False

                    print(message[7:])
                elif message.startswith("CHATLATER"):
                    online_flag = False
                    if not omit_message:
                        print("this user is not online now, your message will not be received")
                    omit_message = True
                elif message.startswith("EXIT"):
                    return
                elif message.startswith("SWITCH"):
                    global restart
                    restart = True
                    return    
                elif message.startswith("CONFIRMDELETED"):
                    raise delete_account_exception("")
                elif message.startswith("TALKTODELETED"):
                    print("the other user deleted its account, choose another user to talk to (i.e. type '\switch')")

            except delete_account_exception:
                return delete_account_exception("bye bye~")
            except Exception as e:
                #raise Exception("line 674 reconnection")
                if self.client:
                    self.client.close()
                print("line 690 ", e)
                print("start to reconnect")
                self.reconnect()
                self.receive_messages()



if __name__ == '__main__':
    client = Client(sys.argv[1])
