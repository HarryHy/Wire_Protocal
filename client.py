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

# def check_dupname():

def login():
    """
    Send LOGIN message to the server and prompt user to input username and password
    """
    try:
        # Send the 'LOGIN' command to the server
        client.send('LOGIN'.encode('ascii'))
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
        if client:
            client.close()
    
    


def signup():  
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
            client.send(('SIGNUP '+username).encode('ascii'))
            dup_message = client.recv(1024).decode('ascii')
            if dup_message == "DUPNAME":
                print("Username already exists! Change to another one.")
            elif dup_message == "NONDUPNAME":
                password = input("Create your password: ")
                client.send(password.encode('ascii'))
                break

        except Exception as e:
            print('Error Occurred: ', e)

        

    
def listAccounts():
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
            client.send('LIST ALL'.encode('ascii'))
            break
        elif option == "2":
            # Ask the user to input the search pattern
            pattern = input("Input your search pattern: ")
            client.send(('LIST '+pattern).encode('ascii'))
            break
        else: 
            print("Invalid option, choose again")
    
    response = client.recv(1024).decode('ascii')
    if response == "NOMATCHED":
        print("No matched account found")
    elif response == "MATCHED":
        client.send("SENDMATCHED".encode('ascii'))
        # Receive list of matched accounts from server
        list_bytes = client.recv(4096)
        list_accounts = pickle.loads(list_bytes)
        # Print each account in the list
        for a in list_accounts:
            print(a)
        

def receive():
    """
    Receive messages from the server.
    """
    try:
        while True:
            global stop
            if stop: break
            print("in the checking username step")
        
            message = client.recv(1024).decode('ascii')
            if message == "USERNAME":
                client.send(username.encode('ascii'))
                next_message = client.recv(1024).decode('ascii')
                if next_message == 'PASSWORD':
                    client.send(password.encode('ascii'))
                    check_state = client.recv(1024).decode('ascii')
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
        if client:
            client.close()
    choose_operations()

def choose_operations():
    """
    Displays a menu of options to the user (sign in, sign up, or list existing accounts), waits for the user to make a choice, and then calls the appropriate function based on the user's choice. 
    """
    while True:
        option = input("(1)Sign in\n(2)Sign up\n(3)List existing accounts\n")
        if option == "1":
            login()
            break
        elif option == "2":
            signup()
        elif option == "3":
            listAccounts()
        else: 
            print("Invalid option, choose again")
    receive()

def choose_talkto():
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

def start_conversation():
    """
    Starts a conversation with another user, by first receiving conversation history. 
    If there are any queued messages, it receives them and displays them. 
    Then, it sends a message to the server to start the chat, and starts two threads to handle writing and receiving messages.
    """
    choose_talkto()
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
        write_thread = threading.Thread(target=write_messages)
        write_thread.start()
        global receive_thread
        receive_thread = threading.Thread(target=receive_messages)
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

def write_messages():
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

        
        start_conversation()
        
    except Exception as e:
        print('Error Occurred: ', e)


def receive_messages():
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

        except delete_account_exception:
            return delete_account_exception("bye bye~")

        except Exception as e:
            print('Error Occurred: ', e)




def main():
    try:
        os.system("cls||clear")
        global client
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # connect to the host
        client.connect((host, port))
        # finished login here
        choose_operations()  
        start_conversation()
    except:
        if client:
            client.close()



if __name__ == '__main__':
    parse = argparse.ArgumentParser()
    parse.add_argument('-u', dest = 'host' , help = '127.0.0.1')
    parse.add_argument('-p', dest = 'port' ,type=int,help = '9999')
    args = parse.parse_args()
    global host
    global port

    host = args.host
    port = args.port
    main()
