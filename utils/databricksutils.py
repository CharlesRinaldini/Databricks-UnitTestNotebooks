import requests
import json
import time

class instance:

    def __init__(self, url, token, cluster):
        self.url = url
        self.token = token
        self.existingClusterId = cluster
        self.runId = None
        self._runsSubmitUri = self.url + "/api/2.0/jobs/runs/submit"
        self._runsGetUri = None
        self._runsOutputUri = None
        self._runState = None
        self._runPageUrl = None
        self._resultState = None
        self._runOutput = None

    def runNotebook(self, path, args):
        body = {
          "run_name": "Test Run from Unit Test Project",
          "existing_cluster_id": "" + self.existingClusterId + "",
          "timeout_seconds": 120,
          "notebook_task": {
            "notebook_path": "" + path + "",
            "base_parameters": {}
          }
        }

        if len(args) > 0:
            body["notebook_task"]["base_parameters"] = args

        response = requests.post(
            self._runsSubmitUri,
            headers={'Authorization': 'Bearer %s' % self.token},
            json=body
        )
        rJson = json.loads(response.text)

        self.runId = rJson["run_id"]
        self._runsGetUri = self.url + "/api/2.0/jobs/runs/get?run_id=" + str(self.runId)

        runState = "PENDING"

        while runState != "TERMINATED":
            runState = self.getRunState()
            if runState != "TERMINATED":
                print(f"Run state currently: {runState}. Waiting 20 seconds...")
                time.sleep(20)

        if self._resultState == "SUCCESS":
            self._runsOutputUri = self.url + "/api/2.0/jobs/runs/get-output?run_id=" + str(self.runId)
            response = requests.get(
                self._runsOutputUri,
                headers={'Authorization': 'Bearer %s' % self.token}
            )
            rJson = json.loads(response.text)
            self._runOutput = rJson["notebook_output"]["result"]
        else:
            self._runOutput = "Notebook terminated unsuccessfully"

    def getRun(self):
        response = requests.get(
            self._runsGetUri,
            headers={'Authorization': 'Bearer %s' % self.token}
        )
        rJson = json.loads(response.text)
        self._runState = rJson["state"]["life_cycle_state"]
        if self._runState == "TERMINATED":
            self._runPageUrl = rJson["run_page_url"]
            self._resultState = rJson["state"]["result_state"]

    def getRunState(self):
        self.getRun()
        return self._runState

    def getRunPageUrl(self):
        return self._runPageUrl

    def getRunResultState(self):
        return self._resultState

    def getRunOutput(self):
        return self._runOutput