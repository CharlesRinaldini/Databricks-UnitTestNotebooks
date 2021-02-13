import json
import os

class util:

    def __init__(self):
        fileDir = os.path.dirname(os.path.realpath('__file__'))
        fileName = os.path.join(fileDir, "configs\\.databricks-connect")
        f = open(fileName)
        data = json.load(f)
        self._host = data["host"]
        self._port = data["port"]
        self._token = data["token"]
        self._cluster_id = data["cluster_id"]
        self._org_id = data["org_id"]

    def getHost(self):
        return self._host

    def getPort(self):
        return self._port

    def getToken(self):
        return self._token

    def getClusterId(self):
        return self._cluster_id
        
    def getOrgId(self):
        return self._org_id