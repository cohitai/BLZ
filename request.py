import requests

url = 'http://localhost:5000/results'
r = requests.post(url, json={'DocId': 91192})

print(r)
