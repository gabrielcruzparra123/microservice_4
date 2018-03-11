#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Flask, request #import main Flask class and request object
import pika 
import MySQLdb
import json
import cgi 

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


class Microservice():

    @staticmethod
    def microserviceLogic (nombre,estado):

        try:
            db = MySQLdb.connect(host="18.221.110.33", user="root", passwd="uniandes1", db="microservices",charset='utf8',use_unicode=True)        
            cur = db.cursor()
            query = ("SELECT * FROM material WHERE nombre = %s")
            cur.execute(query, [nombre])
            rows = cur.fetchall()
            i=0
            for row in rows:
                i=i+1
            if i<=0:
                query = "INSERT INTO material (nombre , estado) VALUES("
                query = query+"'"+nombre+"'"+','+"'"+estado+"'"+");"
                cur.execute(query)
                db.commit()
                response = json.dumps({"id":str(cur.lastrowid)  ,"nombre": nombre, "estado":estado,"action":1, "message":'Material persistido'}, indent=4, sort_keys=True, cls=DecimalEncoder )
            else:
                response =json.dumps({"id":str(cur.lastrowid)  ,"nombre": nombre, "estado":estado,"action":0, "message":'Material existente'}, indent=4, sort_keys=True, cls=DecimalEncoder )
            
        except IOError as e:
            db.rollback()
            db.close()
            return "Error BD: ".format(e.errno, e.strerror)


        db.close() 
        print("id persistido: "+str(cur.lastrowid))
        return response 



    @staticmethod
    def queuePublishMessage (data):
        try:

            message = { "actionQueue":1,"data":data}
            credentials = pika.PlainCredentials('test', 'test')
            parameters = pika.ConnectionParameters('192.168.50.4',5672,'/',credentials)
            connection = pika.BlockingConnection(parameters)

            channel = connection.channel()
            channel.queue_declare(queue='micro_sv')

            channel.basic_publish(exchange='',routing_key='micro_sv',body=json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder))
            connection.close()



            return json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder)

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
        #msg = Microservice.queuePublishMessage(data)
        
        response = {} 
        response['material'] = json.loads(data)
        # aqui irá el mensaje de Queue
        response['msg'] = "Metodo registrar material finalizado OK"
        return  json.dumps(response, indent=4, sort_keys=True, cls=DecimalEncoder)

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=5003)