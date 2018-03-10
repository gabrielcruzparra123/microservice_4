#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Flask, request #import main Flask class and request object
import pika 
import MySQLdb
import json
import cgi 


class Microservice():

    @staticmethod
    def microserviceLogic (nombre,estado):

        try:
            db = MySQLdb.connect(host="18.221.110.33", user="root", passwd="uniandes1", db="microservices",charset='utf8',use_unicode=True)        
            cur = db.cursor()
            query = "INSERT INTO material (nombre , estado) VALUES("
            query = query+"'"+nombre+"'"+','+"'"+estado+"'"+");"
            cur.execute(query)
            db.commit()
            
        except IOError as e:
            db.rollback()
            db.close()
            return "Error BD: ".format(e.errno, e.strerror)


        db.close() 
        print("id persistido: "+str(cur.lastrowid))
        return {"id":str(cur.lastrowid)  ,"nombre": nombre} 



    @staticmethod
    def queuePublishMessage (id):
        try:

            credentials = pika.PlainCredentials('test', 'test')
            parameters = pika.ConnectionParameters('192.168.50.4',5672,'/',credentials)
            connection = pika.BlockingConnection(parameters)

            channel = connection.channel()
            channel.queue_declare(queue='micro_sv')
            channel.basic_publish(exchange='',routing_key='micro_sv',body='Publish :'+id)
            connection.close()

            return "Message Sent to the Queue. Publish: "+id

        except IOError as e:
            print ("Error Queue: ".format(e.errno, e.strerror))

app = Flask(__name__)

@app.route('/microservicepython_4/registrar_material', methods=['POST'])
def registrar_categoria():

    if request.method == "POST":

        req_data = request.get_json()

        nombre = req_data['nombre']
        estado = req_data['estado']
        
                        
        data = Microservice.microserviceLogic(nombre,estado)
        #msg = Microservice.queuePublishMessage(data["id"])
        
        response = {} 
        response['material'] = "Material "+data["nombre"]+" persistido."
       # response['msg'] = msg
        return  json.dumps(response)

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=5003)