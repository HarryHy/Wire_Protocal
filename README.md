# wire_protocols
The wire_protocols is an implementation of a wire protocol that can be used for inter-process communication or data transfer between different devices. 

# Installation & Setup

## Pip
```
pip install pycopy-fnmatch
```

## Manual Installation
- Python 3.7+
- grpcio
- protobuf

# Running the chat box:
To run the chat box, follow these steps:
## Clone the repository to your local machine:
```
git clone https://github.com/XinyuKang/wire_protocols.git
```

## Run server
Start the server by opening a terminal and running:
```
python server.py
```
By default, the server will listen on `127.0.0.1:5555`, you can change the host and port manually inside the `server.py` script

## Run client
Start a client by opening a new terminal and running:
```
python client.py -u user_host -p user_port
```
Replace user_host and user_port with the host and port of the server you want to connect to. Make sure they match the server's host and port, otherwise the connection will fail.

## Login
- If you haven't created an account yet, choose option (2) to create a new account.
- If the account username existed in the list of accounts, an error message will occur.
- If you already have an account, choose option (1) to sign in. You will be asked to enter your username and password. If the password matches the username, the login will be successful.
- If you want to see a list of existing accounts, choose option (3) to have a full list of usernames, or use wildcard to have a filtered list.

## Choose the user to talk to
- After the login step, the terminal will ask you to choose a user to talk to. Input the correct username of the person and start your chat
- Once the talkto user is chosen, the history of the previous messages will be displayed first, and then you can start typing your messages.

## Switch user to talk to
- If you want to switch to another user while chatting, simply type `\switch` and return to the choose talkto user page. 

## Quit the chatroom
- If you want to Quit this chatroom and close your account, simply type `\exit` during the chat. 
- You can also use keyboard interrupt to quit this program. This will not close the server. 

## Delete your account
- If you want to delete your account, simply type `\delete` during the chat.


## To test the chat box:
To test the chat box, you need to install pytest and mock:
```
pip install pytest
pip install mock
```

We have developed a comprehensive suite of unit tests to verify the functionality of both our client and server. However, due to the involvement of concurrency and parallel computing, we have also conducted manual testing to simulate a range of different scenarios. For instance, we have intentionally introduced errors (such as keyboard interrupt, internet connection errors etc) into the login, signup, user selection, and chatting steps for one client to ensure robustness and resilience in these critical areas.

## Design decisions: 
- Our design philosophy centers around creating a clear separation between the server and clients. To achieve this, we have implemented a design that ensures all chat histories and verifications occur solely within the server, without providing any additional information to the clients. Moreover, our system is designed such that errors on the client-side will not cause the server to crash. We have also employed the use of try-except blocks to handle any errors that may arise during runtime, ensuring the stability and reliability of the system.
- Our client has two main components. The first is the login stage, which allows users to log in, sign up, and view a list of all accounts. Upon completion of these actions, the user is recognized as an 'online user' on the server side. The second component is the chat stage. At the outset, the user selects a chat partner, which can include themselves, and the server loads the message history between the two parties since their last conversation. Following this, the user can send messages to their chat partner, even if that partner is not currently online. To facilitate this, the start_conversation function launches two threads, one for writing messages and the other for receiving messages. One of our key design decisions was to ensure that the user receives all messages if they are online. For instance, if Alice is chatting with Bob, she can still receive messages from other users such as Charles. Users can switch to a different chat partner using the switch function. To enable account switching and deletion, we have implemented exception handling to facilitate the restart of the process.
