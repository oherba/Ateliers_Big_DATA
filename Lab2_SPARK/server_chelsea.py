import socket
import time
import pandas as pd
from GoogleNews import GoogleNews

googlenews = GoogleNews(period='30min')

# Search for news about specific topic
googlenews.search('chelsea')

# Get results
result = googlenews.result()
df = pd.DataFrame(result)
df = df['desc']
# Accept client connection
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
server.bind(('localhost', 4523))
server.listen()

client, address = server.accept()
print("New connection from: " + str(address))

# Process each row of data as a batch
for row in df:
    # Send data to the client
    client.sendall((row + "\n").encode())
    print("Sending:", row)
    time.sleep(30)
    print("OK...")

# Close the client connection
client.close()