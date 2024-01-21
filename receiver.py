import os
import sys
import time
import json
import glob
import base64
import hashlib
import cursor
import paho.mqtt.client as mqtt

CLIENT_ID   = 'client_2'
BROKER      = 'test.mosquitto.org'
PORT        = 1883
USERNAME    = 'testuser'
PASSWORD    = '12345678'
TOPIC       = 'files'
BUF_SIZE    = 1024
TEMPDIR     = "temp"

def loading_dots():
    cursor.hide()
    count = 0
    while True:
        sys.stdout.write('.')
        sys.stdout.flush()
        time.sleep(1)
        count += 1

        if count == 3:
            sys.stdout.write('\b\b\b')  # Move the cursor back
            sys.stdout.write('   \b\b\b')  # Overwrite the last two dots with spaces
            sys.stdout.flush()
            count = 0

def connect():
    def on_connect(client: mqtt.Client, userdata, flags, rc):
        if rc == 0:
            pass
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

def store_data(data, hash_value, chunk_number, file_name):
    """ save data to temp file
        and send recieved chunknumber
    """
    if hashlib.md5(data.encode()).hexdigest() == hash_value:
        fname = f"{TEMPDIR}/{file_name}.temp"
        f = open(fname, "ab")
        if chunk_number == 0:
            f = open(fname, "wb")

        try:
            f.write(base64.b64decode(data))
        except Exception as e:
            print(f"ERR while writing on {fname}: {e}")
            return
        finally:
            f.close()

        print(f"saved chunk: {chunk_number} to {fname}")
        client.publish(
            TOPIC, 
            to_json({"chunknumber": chunk_number})
        )

def rename_file(file_name, hash_value):
    """ check temp file and rename to original
    """
    for file in os.listdir(TEMPDIR):
        if md5_hash_generator(f'{TEMPDIR}/{file}') == hash_value:
            os.rename(f'{TEMPDIR}/{file}', f'{TEMPDIR}/{file_name}')

    for f in glob.glob(f'{TEMPDIR}/*.temp'):
        os.remove(f)

    print(f"OK: saved file {file_name}")

def on_subscribe(client, userdata, mid, granted_qos):
    print(f"Subscribed to topic with QoS {granted_qos}")

def process_file(msg):
    """ convert msg to json,
        send data to file
    """
    try:
        # if type(msg) is bytes:
        #     msg = msg.decode()
        payload = msg.payload.decode()
        json_data = json.loads(payload)
    except Exception as e:
        print(f"ERR msg2json: {e}")

    try:
        if json_data["end"] is False:
            store_data(
                json_data["chunk_data"],
                json_data["chunk_hash"],
                json_data["chunk_number"],
                json_data["file_name"]
            )
        if json_data["end"] is True:
            rename_file(json_data["file_name"], json_data["hash_value"])
    except Exception as e:
        print("ERR: parse json", e)

def on_message(client, userdata, msg):
    print("on message callback")
    process_file(
        msg.payload
    )

def subscribe(client: mqtt.Client):
    client.on_subscribe = on_subscribe
    client.on_message   = on_message
    client.subscribe(TOPIC)

if __name__ == '__main__':
    client = connect()
    subscribe(client)
    client.loop_forever()