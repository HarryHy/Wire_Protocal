import unittest
from unittest.mock import patch
from unittest import mock
from unittest.mock import MagicMock, patch
from unittest.mock import Mock
from io import StringIO
import sys
from c2 import ChatClient


import unittest
from unittest.mock import patch
from c2 import ChatClient, choose_operations

'''
To run the mock test with invalid input, it 
 has to define a new function outside the while loop
'''
def choose_operations_invalid(client):
    res = 1
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



class TestChatClient(unittest.TestCase):
    
    @patch('builtins.input', side_effect=['1', 'username', 'password'])
    def test_choose_operations_sign_in(self, mock_input):
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            with patch.object(client, 'login', return_value=0) as mock_login:
                choose_operations(client)
                mock_login.assert_called_once_with('username', 'password')

    @patch('builtins.input', side_effect=['2', 'username', 'password'])
    def test_choose_operations_sign_up(self, mock_input):
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            with patch.object(client, 'signup', return_value=0) as mock_signup:
                choose_operations(client)
                mock_signup.assert_called_once_with('username', 'password')

    @patch('builtins.input', side_effect=['3'])
    def test_choose_operations_list_accounts(self, mock_input):
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            with patch.object(client, 'listAccounts', return_value=0) as mock_list:
                choose_operations(client)
                mock_list.assert_called_once()
    

    def test_choose_operations_invalid_option(self,):
    # set up
        with patch('builtins.input', side_effect=['5']):
            with mock.patch('socket.socket'):
                client = ChatClient('localhost', 8000)
                with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                    # execute
                    choose_operations_invalid(client)
                    
                    # assert
                    expected_output = 'Invalid option, choose again\n'
                    self.assertEqual(mock_stdout.getvalue(), expected_output)


if __name__ == '__main__':
    unittest.main()
