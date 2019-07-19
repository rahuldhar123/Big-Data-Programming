from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket,time,json


consumer_key = 'xRiLoUoOeiWABfkaSOb26Fjty'
consumer_secret = 'vy0n9VaX72sDW4HK8X6qNcUU3bNzpy1YEXUloqwYpSc79aVhA9'
access_token = '1090836590069465091-BtqFwoKMF8n1FnFCChcoQyqn1PwK6o'
access_secret = 'Mq3VpYBZ3py6VnceJ3N45bjpM5QtW0gRx66c4gccKHmmr'


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'])
            self.client_socket.send(msg['text'])
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendmsg(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['Worldcup','Fifa','Annabelle','Spiderman'])


if __name__ == "__main__":
    s = socket.socket()  # Socket Creation
    host = "localhost"  # Get local machine name
    port = 9999  # Reserving port
    s.bind((host, port))
    print("Listening on port: %s" % str(port))
    s.listen(5)  #Waiting for client connection.
    c, addr = s.accept()  # Establish connection with client.
    print("Request Received From: " + str(addr))
    time.sleep(5)
    sendmsg(c)