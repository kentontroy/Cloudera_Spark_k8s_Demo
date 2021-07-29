import base64
import http.client
from urllib import request, parse, error 

headers = {
 'api_key': 'e13626d03d8e4c03ac07f95541b3091b',
}

try:
  conn = http.client.HTTPSConnection('api.wmata.com')
  conn.request("GET", "/Rail.svc/json/jStations", "", headers)
  response = conn.getresponse()
  data = response.read()

  with open('stations.json', 'wb') as f:
    f.write(data)

  conn.close()

except Exception as e:
  print(repr(e))
