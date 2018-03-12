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
            db = MySQLdb.connect(host="18.188.72.8", user="root", passwd="uniandes1", db="microservices",charset='utf8',use_unicode=True)        
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
            response = json.dumps({"id":0, "nombre": nombre, "estado":estado,"action":-1, "message":'Imposible persistir el objeto'}, indent=4, sort_keys=True, cls=DecimalEncoder )
            return response


        db.close() 
        print("id persistido: "+str(cur.lastrowid))
        return response 



    @staticmethod
    def queuePublishMessage (data):
        try:

            message = { "actionMaterialQueue":1,"dataMaterialQueue":data}
            credentials = pika.PlainCredentials('test', 'test')
            parameters = pika.ConnectionParameters('192.168.50.6',5672,'/',credentials)
            connection = pika.BlockingConnection(parameters)

            channel = connection.channel()
            channel.queue_declare(queue='micro_sv')

            channel.basic_publish(exchange='',routing_key='micro_sv',
                body=json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder))
            connection.close()



            return json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder)

        except IOError as e:
            message = { "actionQueue":0,"data":json.lodas(data)}
            return json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder)     


    @staticmethod
    def buildDummyCategory():
        try:
            category ={"id":1  ,"nombre": "Sillas", "estado":'A',"action":1, "message":'Categoria persistida'}
            response = {"actionCategoryQueue":1, "dataCategory":json.dumps(category,indent=4, sort_keys=True, cls=DecimalEncoder )}
            return json.dumps(response,indent=4, sort_keys=True, cls=DecimalEncoder)

        except IOError as e:
            print("Error inesperado construyendo categoria")

    @staticmethod
    def queueConsumeMessageSaga(body):
        try:
            bodyQueue =json.loads(body)
            actionCategoryQueue = bodyQueue['actionCategoryQueue']
            actionCategory = bodyQueue ['dataCategory']['action']
            if actionCategoryQueue == 1:
                if actionCategory == 1:
                    message = { "actionMaterialQueue":1,"dataCategoryQueue":body}
                    return json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder)
                else:
                   message = { "actionMaterialQueue":0,"dataCategoryQueue":body}
                   return  json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder) 
            else:       
                message = { "actionMaterialQueue":0,"dataCategoryQueue":body}
                return  json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder)
                    
        except IOError as e:
            message = { "actionMaterialQueue":0,"dataCategoryQueue":body}
            return  json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder)            

    @staticmethod 
    def queueConsumeMessage():
        try:
            credentials = pika.PlainCredentials('test', 'test')
            parameters = pika.ConnectionParameters('192.168.50.5',5672,'/',credentials)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            method_frame, header_frame, body = channel.basic_get('micro_sv')
            if method_frame:
                response = Microservice.queueConsumeMessageSaga(body)
                channel.basic_ack(method_frame.delivery_tag)
                return response
            else:
                message = { "actionMaterialQueue":0,"dataCategoryQueue":body}
                return json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder)
            
        except IOError as e:
            message = { "actionMaterialQueue":0,"dataCategoryQueue":body}
            return json.dumps(message, indent=4, sort_keys=True, cls=DecimalEncoder)

             

app = Flask(__name__)

@app.route('/microservicepython_4/registrar_material', methods=['POST'])
def registrar_material():

    if request.method == "POST":

        req_data = request.get_json()

        nombre = req_data['nombre']
        estado = req_data['estado']
        # Para pruebas se crea este método pero debe consumirse desde la cola
        #body = Microservice.buildDummyCategory()
        # este método se consume directamente pero se debe consumir desde la cola
        msg = json.loads(Microservice.queueConsumeMessage())

        actionMaterialQueue = msg['actionMaterialQueue']
        if actionMaterialQueue == 1:
            print("Encuentra actionMaterialQueue: "+str(actionMaterialQueue))    
            data = Microservice.microserviceLogic(nombre,estado)
            response = {} 
            response['material'] = json.loads(data)
            response['msg'] = msg
            if response['material']['action'] != 1:
                content = json.loads(msg['dataCategoryQueue'])
                print content ['dataCategory']['id']
                db = MySQLdb.connect(host="18.188.72.8", user="root", passwd="uniandes1", db="microservices",charset='utf8',use_unicode=True)        
                cur = db.cursor()
                query = ("DELETE FROM categoria WHERE  id_categoria= %s")
                cur.execute(query,[content ['dataCategory']['id']])
                db.commit()
            else:
                print ('bien :)')
                
        else:
            response = {} 
            response['material'] = json.dumps({"id":0, "nombre":nombre, "estado":estado, "action":0, "message":"No es posible persitir material, por fallos en categoria, se aplica compensacion"}, indent=4, sort_keys=True, cls=DecimalEncoder)
            # aqui irá el mensaje de Queue
            response['msg'] = msg
            content = json.loads(msg['dataCategoryQueue'])
            print content ['dataCategory']['id']
            db = MySQLdb.connect(host="18.188.72.8", user="root", passwd="uniandes1", db="microservices",charset='utf8',use_unicode=True)        
            cur = db.cursor()
            query = ("DELETE FROM categoria WHERE  id_categoria= %s")
            cur.execute(query,[content ['dataCategory']['id']])
            db.commit()

        # quitar esta linea de comentarios para publicar mensaje a productos    
        print(response)
        Microservice.queuePublishMessage(response)   
        return  json.dumps(response, indent=4, sort_keys=True, cls=DecimalEncoder)   

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=5003)