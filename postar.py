from kafka import KafkaConsumer
import requests
import json, os
import twitter

def postar_twitter(msg):
  # Obtain keys from the following URL by creating a new app.
  # https://apps.twitter.com/
  consumer_key = ""
  consumer_secret = ""
  access_token_key = ""
  access_token_secret = ""
  #api = twitter.Api(consumer_key, consumer_secret, access_token_key, access_token_secret)
  status = msg #api.PostUpdate(msg)
  print("Status = " + status)
  return status
  
def handler(event, context):
    print("Evento recebido = " + str(event))
    # Decode UTF-8 bytes to Unicode, and convert single quotes 
    # to double quotes to make it valid JSON
    texto=event['data'].decode('utf-8').replace("'", '"')
    print('Texto = '+ texto)
    dados = json.loads(texto)
    
    try:        
        if not 'msg' in dados:
          return 'Campo vazio : msg'
        # ler a msg
        texto = dados['msg']
        print("Texto = "+ texto)
        return postar_twitter(texto)
        
    except Exception as e:
        erro = "Erro na function: " + repr(e);
        print(erro)
        return erro
