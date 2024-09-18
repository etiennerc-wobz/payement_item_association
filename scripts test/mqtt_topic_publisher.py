import os
import yaml 
import logging
import threading
import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime

class MQTTInterface:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        module_dir = os.path.dirname(__file__)
        with open(os.path.join(module_dir,'../config/mqtt_params.yaml')) as yaml_file:
            try :
                config = yaml.safe_load(yaml_file)
            except yaml.YAMLError as exc:
                print(exc)

        self.broker_address = config['broker_address']
        self.port = config['port']
        self.topic_payment = config['topic_payment']
        self.topic_item = config['topic_item']
        self.username = config['username']
        self.password = config['password']
        self.rpi_id = config['rpi_id']
        self.topic_payment += "/" + self.rpi_id
        print("MQTT Subscriber start with params :")
        print(f"broker_address : {self.broker_address}")
        print(f"port : {self.port}")
        print(f"topic_payment : {self.topic_payment}")
        print(f"topic_item : {self.topic_item}")
        print(f"username : {self.username}")
        print(f"password : {self.password}")
        print(f"rpi_id : {self.rpi_id}")
        self.client = mqtt.Client()
        
        # Assignation des callbacks
        self.client.username_pw_set(self.username, self.password)
        self.client.on_connect = self.on_connect

    def on_connect(self, client, userdata, flags, rc):
        """Callback pour la connexion au broker."""
        print(f"Connecté au broker avec le code de retour {rc}")
        # Souscrire au topic avec QoS 1
        if rc == 0:
            print("Connecté au broker MQTT avec succès")
        else:
            print(f"Erreur de connexion. Code retour : {rc}")

    def start(self):
        """Démarrer la boucle réseau MQTT dans un thread séparé."""
        self.client.connect(self.broker_address, self.port)
        thread = threading.Thread(target=self.client.loop_forever)
        thread.daemon = True  # Assurez-vous que le thread se termine avec le programme principal
        thread.start()

    def stop(self):
        """Arrêter la souscription MQTT."""
        self.client.disconnect()
        print("Déconnexion du client MQTT.")

    def publish(self, topic, message):
        """Publier un message sur un topic."""
        result = self.client.publish(topic, message)
        status = result.rc

        if status == 0:
            print(f"Message envoyé avec succès à {topic}")
        else:
            print(f"Échec de l'envoi du message à {topic}. Code retour : {status}")

    def pubItem(self,data):
        """Publier un message sur un topic."""
        message = json.dumps(data)
        self.publish(self.topic_item, message)
    
    def pubTransaction(self,data):
        """Publier un message sur un topic."""
        message = json.dumps(data)
        self.publish(self.topic_payment, message)


# Exécution du module principal pour tester
if __name__ == "__main__":

    mqtt_interface = MQTTInterface()
    mqtt_interface.start()
    transaction_1 = {
        'transactionId' : 1,
        'count' : 3,
        'createdAt' : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    time.sleep(2)
    item_1 = {
        'item_list' : [11,12,13],
        'time_stamp' : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    time.sleep(2)
    transaction_2 = {
        'transactionId' : 2,
        'count' : 3,
        'createdAt' : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    time.sleep(2)
    item_2 = {
        'item_list' : [21,22,23],
        'time_stamp' : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    time.sleep(2)
    transaction_3 = {
        'transactionId' : 3,
        'count' : 3,
        'createdAt' : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    time.sleep(2)
    item_3 = {
        'item_list' : [31,32,33],
        'time_stamp' : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    mqtt_interface.pubItem(item_1)
    time.sleep(1)
    mqtt_interface.pubItem(item_2)
    time.sleep(1)
    mqtt_interface.pubTransaction(transaction_2)
    time.sleep(1)
    mqtt_interface.pubItem(item_3)
    time.sleep(1)
    mqtt_interface.pubTransaction(transaction_1)
    time.sleep(1)
    mqtt_interface.pubTransaction(transaction_3)
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Arrêt du script.")
        mqtt_interface.stop()
