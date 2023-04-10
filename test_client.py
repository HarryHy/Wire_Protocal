import unittest
import socket
from unittest import mock
from unittest.mock import MagicMock, patch
from unittest.mock import Mock
from c2 import ChatClient
import pickle
import threading
from io import StringIO


class TestChatClient(unittest.TestCase):
    # def setUp(self):
    #     self.client = ChatClient("localhost", 56789)

    def test_init(self):
        with mock.patch('socket.socket'):
            client = ChatClient("localhost", 56789)
            self.assertEqual(client.server_host, "localhost")
            self.assertEqual(client.server_port, 56789)
            self.assertEqual(client.client, socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            client.client.connect.assert_called_once_with(("localhost", 56789))

    def test_send_message(self):
        with mock.patch('socket.socket'):
            client = ChatClient("localhost", 12345)
            client.username = "alice"
            client.send("hello")
            client.client.send.assert_called_once_with("hello")

    def test_close(self):
        with mock.patch('socket.socket'):
            client = ChatClient("localhost", 12345)
            client.username = "alice"
            client.close()
            client.client.close.assert_called_once()
    
    def test_login_with_valid_credentials(self):
        # Arrange
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            client.client.recv = Mock(side_effect=[
                'USERNAME'.encode('ascii'),
                'PASSWORD'.encode('ascii'),
                'OK'.encode('ascii')
            ])
            
            # Act
            result = client.login('testuser', 'testpassword')
            
            # Assert
            self.assertEqual(result, 0)
            self.assertEqual(client.username, 'testuser')

    def test_login_with_invalid_username(self):
        # Arrange
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            client.client.recv = Mock(side_effect=[
                'USERNAME'.encode('ascii'),
                'PASSWORD'.encode('ascii'),
                'NOUSER'.encode('ascii')
            ])
            
            # Act
            result = client.login('testuser', 'testpassword')
            
            # Assert
            self.assertEqual(result, 1)
            self.assertIsNone(client.username)

    def test_login_with_invalid_password(self):
        # Arrange
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            client.client.recv = Mock(side_effect=[
                'USERNAME'.encode('ascii'),
                'PASSWORD'.encode('ascii'),
                'REJECT'.encode('ascii')
            ])
            
            # Act
            result = client.login('testuser', 'testpassword')
            
            # Assert
            self.assertEqual(result, 1)
            self.assertIsNone(client.username)

    def test_list_account_option1(self):
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            client.list_option1()
            client.client.send.assert_called_once_with(b"LIST ALL")

    def test_list_account_option2(self):
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            client.list_option2("xin*")
            client.client.send.assert_called_once_with(b"LIST xin*")

    def dump(self, client):
        while True:
                client.client.recv = Mock(side_effect=[
                pickle.dumps(['user1', 'user2', 'user3'])
            ])

    def test_receive_from_list_account_matched(self):
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            dump_thread = threading.Thread(target=self.dump, args=(client,))
            dump_thread.start()
            
            result = client.receive_from_listAccounts("MATCHED")
            self.assertEqual(result, 1)
            client.client.send.assert_called_once_with(b"SENDMATCHED")
            

    def test_receive_from_list_account_nomatched(self):
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            # dump_thread = threading.Thread(target=self.dump, args=(client,))
            # dump_thread.start()
            with patch('sys.stdout', new = StringIO()) as fake_out:
                result = client.receive_from_listAccounts("NOMATCHED")
                self.assertEqual(result, 1)
                self.assertEqual(fake_out.getvalue(), "No matched account found\n")
                
    @mock.patch('builtins.input', side_effect=['testusername', 'testpassword'])
    def test_signup_with_new_username(self, mock_input):
        # Arrange
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            client.client.recv = mock.Mock(side_effect=[
                'SIGNUP testusername'.encode('ascii'),
                'NONDUPNAME'.encode('ascii')
            ])

            # Act
            result = client.signup('testusername', 'testpassword')

            # Assert
            self.assertEqual(result, 0)

    @mock.patch('builtins.input', side_effect=['testusername', 'testpassword'])
    def test_signup_with_existing_username(self, mock_input):
        # Arrange
        with mock.patch('socket.socket'):
            client = ChatClient('localhost', 8000)
            client.client.recv = mock.Mock(side_effect=[
                'SIGNUP testusername'.encode('ascii'),
                'DUPNAME'.encode('ascii')
            ])

            # Act
            result = client.signup('testusername', 'testpassword')

            # Assert
            self.assertEqual(result, 1)

if __name__ == "__main__":
    #mock_socket = mock.Mock()
    test = TestChatClient()
    test.test_init()
    test.test_send_message()
    test.test_close()
    test.test_login_with_valid_credentials()
    test.test_login_with_invalid_username()
    test.test_login_with_invalid_password()
    test.test_list_account_option1()
    test.test_list_account_option2()
    test.test_receive_from_list_account_nomatched()
    test.test_receive_from_list_account_matched()
    test.test_signup_with_existing_username()
    test.test_signup_with_new_username()
    
