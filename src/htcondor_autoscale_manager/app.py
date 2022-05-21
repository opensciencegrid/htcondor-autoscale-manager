
import os

import htcondor

from flask import Flask
from flask_apscheduler import APScheduler

import htcondor_autoscale_manager.occupancy_metric

app = Flask(__name__)

config = {}
for key, val in os.environ.items():
    if key.startswith("FLASK_"):
        app.config[key[6:]] = val

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

g_metric = 1.0

@scheduler.task("interval", id="metric_update", seconds=60)
def metric_update():
    resource = app.config.get("RESOURCE_NAME")
    if not resource:
        print("RESOURCE_NAME not set - cannot compute metric.")
        return
    with htcondor.SecMan() as sm:
        if 'BEARER_TOKEN' in app.config:
            sm.setToken(htcondor.Token(app.config['BEARER_TOKEN']))
        elif 'BEARER_TOKEN' in os.environ:
            sm.setToken(htcondor.Token(os.environ['BEARER_TOKEN']))
        elif 'BEARER_TOKEN_FILE' in app.config:
            with open(app.config['BEARER_TOKEN_FILE']) as fp:
                sm.setToken(htcondor.Token(fp.read().strip()))
        elif 'BEARER_TOKEN_FILE' in os.environ:
            with open(os.environ['BEARER_TOKEN_FILE']) as fp:
                sm.setToken(htcondor.Token(fp.read().strip()))
        try:
            global g_metric
            g_metric = htcondor_autoscale_manager.occupancy_metric(resource)
        except Exception as exc:
            print(f"Exception occurred during metric update: {exc}")

@app.route("/metrics")
def metrics():
    return f"occupancy {g_metric}\n"

def entry():
    app.run()
