import os
import threading
import socket
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
host = "127.0.0.1"
post = '5555'

class ChatClient:
    def __init__(self, host, port):
        self.server_host = host
        self.server_port = port
        self.username = None
        self.login_err = False
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # connect to the host
        self.client.connect((host, port))
        
    def send(self, mes):
        self.client.send(mes)

    def recv(self, num):
        mes = self.client.recv(num)  
        return mes

    def close(self):
        self.client.close()
    
    def login(self, username, password):
        """
        Send LOGIN message to the server and prompt user to input username and password
        """
        try:
            # Send the 'LOGIN' command to the server
            self.client.send('LOGIN'.encode('ascii'))
            # Clear the console
            os.system("cls||clear")
            # Set the 'stop' flag to False, indicating that the client is still running
            global stop
            stop = False
            while True:
                if stop: break
                print("in loop")
                print("error 1")
                message = self.client.recv(1024).decode('ascii')
                if message == "USERNAME":
                    print("error 2")
                    self.client.send(username.encode('ascii'))
                    next_message = self.client.recv(1024).decode('ascii')
                    if next_message == 'PASSWORD':
                        print("error 3")
                        self.client.send(password.encode('ascii'))
                        check_state = self.client.recv(1024).decode('ascii')
                        if check_state == "REJECT":
                            print("Wrong password! Try again")
                            return 1
                        elif check_state == "NOUSER":
                            print("No such user")
                            return 1
                        else:
                            #print(s)
                            print("Successfully logged in as ", username)
                            self.username = username
                            return 0
                elif message == "FAIL":
                    print("You've reach the attemp limit, connection failed.")
                    return 1
                else: 
                    print(" the message is not on the list")
                    print(message)
                    return 1
        except Exception as e:
            # If an error occurs, print an error message and close the connection to the server
            print('Error Occurred in login: ', e)

    def signup(self, username, password):  
        """
        Handle the process of creating a new account.
        It prompts the user to create a unique username, sends it to the server for duplicate check, and then asks for a password. 
        If the username already exists, it prompts the user to choose another one. 
        """
        os.system('cls||clear')
        # check if username is unique
        while True:
            try:
                # send the username to server for duplicate check
                print("signup 1")
                self.client.send(('SIGNUP '+username).encode('ascii'))
                dup_message = self.client.recv(1024).decode('ascii')
                print("signup 2")
                if dup_message == "DUPNAME":
                    print("Username already exists! Change to another one.")
                    return 1
                elif dup_message == "NONDUPNAME":
                    password = input("Create your password: ")
                    self.client.send(password.encode('ascii'))
                    return 0

            except Exception as e:
                print('Error Occurred: ', e)

    def list_option1(self):
        self.client.send('LIST ALL'.encode('ascii'))

    def list_option2(self, pattern):
        self.client.send(('LIST '+pattern).encode('ascii'))

    def receive_from_listAccounts(self, response):
        
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
        return 1

    
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
                self.list_option1()
                # self.client.send('LIST ALL'.encode('ascii'))
                break
            elif option == "2":
                # Ask the user to input the search pattern
                pattern = input("Input your search pattern: ")
                # self.client.send(('LIST '+pattern).encode('ascii'))
                self.list_option2(pattern)
                break
            else: 
                print("Invalid option, choose again")
        response = self.client.recv(1024).decode('ascii')
        self.receive_from_listAccounts(response)

def choose_operations(client):
    """
    Displays a menu of options to the user (sign in, sign up, or list existing accounts), waits for the user to make a choice, and then calls the appropriate function based on the user's choice. 
    """
    global username
    global password
    res = 1
    while True:
        if res == 0 :
            return
        option = input("(1)Sign in\n(2)Sign up\n(3)List existing accounts\n")
        if option == "1":
            username = input("Enter your username: ")
            password = input("Enter the password: ")
            res = client.login(username, password)
        elif option == "2":
            username = input("Enter your username: ")
            res = client.signup(username, password)
        elif option == "3":
            res = client.listAccounts()
        else: 
            print("Invalid option, choose again")

def choose_talkto(client):
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
        client.send(("TALKTO "+talkto).encode('ascii'))
        next_message = client.recv(1024).decode('ascii')
        if next_message == "VALTALKTO":
            print("next_message is " + next_message)
            print("Start your conversation with "+talkto + "!")
            choose_talk_to_stop = True
            break
        elif next_message == "INVALTALKTO":
            print("The username you were trying to talk to doesn't exist, please try another one.")
        else: 
            print("invalid response of choose_talkto" + next_message)

global receive_begin 
receive_begin = True

def start_conversation(client):
    """
    Starts a conversation with another user, by first receiving conversation history. 
    If there are any queued messages, it receives them and displays them. 
    Then, it sends a message to the server to start the chat, and starts two threads to handle writing and receiving messages.
    """
    #os.system('cls||clear')
    choose_talkto(client)
    client.send('STARTHIST'.encode('ascii'))
    print("finish client.send('STARTHIST'.encode('ascii'))")
    # receive all the queued messages
    flag = client.recv(1024).decode('ascii')
    print("flag is ", flag)
    if flag != "EMPTY":
        list_bytes = client.recv(4096)
        #print("list_bytes is ", list_bytes)
        list_messages = pickle.loads(list_bytes)
        for m in list_messages:
            print(talkto + " : " + m)
    print("--------------start to chat-----------------")
    # after receive the history, start to chat
    client.send('STARTCHAT'.encode('ascii'))
    try:
        global write_thread
        write_thread = threading.Thread(target=write_messages, args= [client])
        write_thread.start()
        global receive_thread
        receive_thread = threading.Thread(target=receive_messages, args= [client])
        receive_thread.start()
    
        
    except restart_conversation_exception:
        print('actually returned to start_conversation')
        write_thread.join()
        receive_thread.join()
        print("There are these number of threads running after restart" + threading.active_count())
        start_conversation()

    except Exception as e:
        print('Error Occurred: ', e)
        write_thread.join()
        receive_thread.join()
        if client:
            client.close()

class restart_conversation_exception(Exception):
    def __init__(self, message):
        print(message)

class delete_account_exception(Exception):
    def __init__(self, message):
        print(message)

def write_messages(client):
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
                client.send(('EXITTT~' + talkto +"~" + username + "~"+ input_message).encode('ascii'))
                print("end of exit")
                return
            elif input_message == "\switch":
                chat_break = True
                client.send(('SWITCH~' + talkto +"~" + username + "~"+ input_message).encode('ascii'))
                print("end of switch")
                raise restart_conversation_exception("restart")
            elif input_message == "\delete":
                # delete self account
                client.send(('DELETE~' + talkto +"~" + username + "~"+ input_message).encode('ascii'))
                # raise delete_account_exception("")
                print("end of delete")
                print("You are forced to log out")
                return
            else:
                print(username + " : " + input_message)
                client.send(('CHATTT~' + talkto +"~" + username + "~"+ input_message).encode('ascii'))
    except restart_conversation_exception:
        #global receive_begin
        #receive_begin = False
        
        start_conversation()
    # except delete_account_exception:
    #     return delete_account_exception("bye bye~")
    except Exception as e:
        print('Error Occurred: ', e)
        #client.close()

def receive_messages(client):
    """
    Receives messages from the server and processes them based on their contents
    """
    online_flag = False
    omit_message = False
    while True:
        try:
            message = client.recv(1024).decode('ascii')
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
                # raise restart_conversation_exception("restart")

        # except restart_conversation_exception:
        #     return restart_conversation_exception("return to start_conversation")
        #     # return

        except delete_account_exception:
            return delete_account_exception("bye bye~")

        except Exception as e:
            print('Error Occurred: ', e)
            #client.close()



def main():

    os.system("cls||clear")
    host = "127.0.0.1"
    post = 5555
    global client    
    client = ChatClient(host, post)
    choose_operations(client)  # finished login here
    print("-------------------------finishing logging in ---------------------------")
    start_conversation(client)
        



if __name__ == '__main__':
    main()
