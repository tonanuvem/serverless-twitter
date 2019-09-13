from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests
import json, os

def kafka(event, context):
    print(event)
    if not 'msg' in event['data']:
        return 'Campo vazio : msg'
    # exemplo de uso: kubeless function call twitter --data '{"msg":"ALUNO: Serverless é show!"}'
    
    try:
        texto=str(event['data'])
        print('Texto = '+ texto)
        msg = event['data']['msg']
        topico = "twitter"
        broker = "kafka.kubeless:9092" 

        # --------
        # USAGE: https://kafka-python.readthedocs.io/en/master/usage.html
        producer = KafkaProducer(bootstrap_servers=[broker])

        # Asynchronous by default
        future = producer.send(topico, texto.encode('utf-8'))
        # Block for 'synchronous' sends
        record_metadata = future.get(timeout=10)
        # Successful result returns assigned partition and offset
        print ('Sucesso no envio. Topico: '+str(record_metadata.topic)+' Particao :' + str(record_metadata.partition) + ' Offset: ' + str(record_metadata.offset))
        return texto;
    
    except Exception as e:
        # Decide what to do if produce request failed...
        print(repr(e))
        #event.extensions.response.statusCode = 400;
        return "Erro na function: " + repr(e);
