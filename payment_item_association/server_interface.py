
import requests
import os
import yaml 
import logging

class serverInterface:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        module_dir = os.path.dirname(__file__)
        with open(os.path.join(module_dir,'../config/server_magicloop_params.yaml')) as yaml_file:
            try :
                config = yaml.safe_load(yaml_file)
            except yaml.YAMLError as exc:
                print(exc)

        self.server_url = config['magic_loop_server_url']
        self.item_association_route = config['item_association_route']
        
        self.logger.info("server interface start with params :")
        self.logger.info(f"magic_loop_server_url : {self.server_url}")
        self.logger.info(f"item_association_route : {self.item_association_route}")
    
    def itemTransactionAssociation(self,data):
        sendin_url = self.server_url + self.item_association_route
        try :
            response = requests.post(sendin_url, json = data,timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as error :
            self.logger.exception(f"Une erreur s'est produite: {error}")
            return 0
        if response.status_code == 201 :
            return 1
        else :
            return 0
        