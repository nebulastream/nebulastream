# Start with a basic flask app webpage.
from flask_socketio import SocketIO, emit
from flask import Flask, render_template, url_for, copy_current_request_context, jsonify
from random import random
from time import sleep
from threading import Thread, Event
import pandas as pd


# Test data file
filename = 'data/shapes_100000.txt'

# Read test file and build routes
df = pd.read_csv(filename, usecols=[0, 1, 2, 4])
points = df.to_records(index=False).tolist()
_routes = {}
for pt in points:
    shape_id = pt[0]
    shape_pt_lat = pt[1]
    shape_pt_lng = pt[2]
    if shape_id not in _routes:
        _routes[shape_id] = [[shape_pt_lat, shape_pt_lng]]
    else:
        _routes[shape_id].append([shape_pt_lat, shape_pt_lng])

curr_pos = {k: 0 for k in _routes.keys()}
print("loading completed")


# Boilerplate
app = Flask(__name__)
socketio = SocketIO(app)

thread = Thread()
thread_stop_event = Event()

# Send the current position
class SendThread(Thread):
    def __init__(self):
        self.delay = 1 # sec
        super(SendThread, self).__init__()

    def sendStatus(self):
        while not thread_stop_event.isSet():
            status = self.updateStatus()
            socketio.emit('newstatus',status, namespace='/rec')
            sleep(self.delay)

    def updateStatus(self):
        status = {}
        for k, v in _routes.items():
#            if(v[curr_pos[k]][0] < float(max_lat) and v[curr_pos[k]][0] > float(min_lat) and v[curr_pos[k]][1] < float(max_lng) and v[curr_pos[k]][1] > float(min_lng)):
            status[k] = v[curr_pos[k]]
            curr_pos[k] += 1
            if curr_pos[k] >= len(v):
                curr_pos[k] = 0
        return status

    def run(self):
        self.sendStatus()


@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('connect', namespace='/rec')
def test_connect():
    print('Client connected')

    # Send Routes
    socketio.emit('routes', _routes, namespace='/rec')

    # Init Thread for status updates of the vehicles
    global thread
    if not thread.isAlive():
        print("Starting Thread")
        thread = SendThread()
        thread.start()

@socketio.on('disconnect', namespace='/rec')
def test_disconnect():
    print('Client disconnected')


if __name__ == '__main__':
    socketio.run(app)
