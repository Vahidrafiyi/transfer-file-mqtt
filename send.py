from paho.mqtt import client as mqtt
import time
import base64
import hashlib
import json
import os

CLIENT_ID   = 'client_1'
BROKER      = 'test.mosquitto.org'
PORT        = 1883
USERNAME    = 'testuser'
PASSWORD    = '12345678'
TOPIC       = 'files'
BUF_SIZE    = 1024

chunk_number = 1


def connect():
    def on_connect(client: mqtt.Client, userdata, flags, rc):
        if rc == 0:
            print('connected')
        else:
            print('did not connect')
    client = mqtt.Client(CLIENT_ID)
    client.on_connect = on_connect
    client.connect(BROKER, PORT)
    return client

def to_json(msg):
    # Convert string to json
    return json.dumps(msg)

def md5_hash_generator(file):
    """hash entire file chunk by chunk through md5 algorithm

    Args:
        file (str): file path

    Returns:
        str: final hash value
    """
    hash_obj = hashlib.md5()
    with open(file, 'rb') as file:
        while True:
            data = file.read(4096)
            if not data:
                break
            hash_obj.update(data)
    return hash_obj.hexdigest()

def publish(client: mqtt.Client, msg):
    try:
        client.publish(TOPIC, to_json(msg), qos=0)
        if msg["end"] is False:
            print(f"send chunk: {msg['chunk_number']}")

    except Exception as e:
        print("ERR publish: ", e)

def send_file(client: mqtt.Client, file):
    """ split, send chunk and wait lock release
    """
    global chunk_number
    time.sleep(2)   # pause for mqtt subscribe
    filesize = os.path.getsize(file)
    filehash = md5_hash_generator(file)

    payload = {
        "file_name" : file,
        "fil_size"  : filesize,
        "file_hash" : filehash,
        "end"       : False
    }

    with open(file, 'rb') as f:
        while True:
            chunk = f.read(BUF_SIZE)
            data = base64.b64encode(chunk)
            if chunk:
                payload.update({
                    "chunk_data"    : data.decode(),
                    "chunk_number"  : chunk_number,
                    "chunk_hash"    : hashlib.md5(data).hexdigest(),
                    "chunk_size"    : len(chunk)})
                publish(client, payload)
                chunk_number += 1
            else:
                del payload["chunk_number"]
                del payload["chunk_data"]
                del payload["chunk_hash"]
                del payload["chunk_size"]
                payload.update(
                    {"end": True}
                )
                print(f"end of transfering file: {file}")
                publish(client, payload)
                break
    time.sleep(1)


if __name__ == '__main__':
    client = connect()
    client.loop_start()
    send_file(client, 'files/sample.txt')
    client.loop_stop()