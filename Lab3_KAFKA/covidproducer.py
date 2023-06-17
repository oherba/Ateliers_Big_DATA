from GoogleNews import GoogleNews
from newspaper import Article
import pandas as pd
import time
from typing_extensions import dataclass_transform
from kafka import KafkaProducer

#googlenews=GoogleNews(start='05/01/2020',end='05/31/2020')
#googlenews=GoogleNews(period='7d')
#googlenews=GoogleNews(period='1h')
googlenews=GoogleNews(period='30min')
googlenews.search('Covid')
result=googlenews.result()
data=pd.DataFrame(result)
data = data["title"]

topic="covid"
producer = KafkaProducer(bootstrap_servers='localhost:9092')
for d in data:
  time.sleep(180) #<=========== 
  future=producer.send(topic, bytes(d, 'utf-8'))
  # Block until a single message is sent (or timeout)
  result = future.get(timeout=60)