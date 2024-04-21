from flask import Flask, render_template
from flask_socketio import SocketIO
from kafka import KafkaConsumer
import json

app = Flask(__name__)
socketio = SocketIO(app)

# Kafka consumer configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'output_to_flask'
consumer = KafkaConsumer(kafka_topic,
                         group_id='output_to_flask_group',
                         bootstrap_servers=[kafka_bootstrap_servers],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))


@app.route('/')
def index():
    return render_template('index.html')


@socketio.on('connect')
def handle_connect():
    print('Client connected')


@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')


def kafka_consumer():
    for message in consumer:
        print('message recieved !',message.value)
        socketio.emit('kafka_message', message.value)


if __name__ == '__main__':
    # Start Kafka consumer in a separate thread
    from threading import Thread
    kafka_thread = Thread(target=kafka_consumer)
    kafka_thread.daemon = True
    kafka_thread.start()

    # Start Flask server
    socketio.run(app, debug=True,host='0.0.0.0')
