import asyncio
import json
import os

from aiokafka import AIOKafkaProducer

loop = asyncio.get_event_loop()


async def send_email():
    list_servers = os.environ.get('BOOTSTRAP_SERVERS', "").split(",")
    if len(list_servers) != 0 and list_servers[0] != '':
        BOOTSTRAP_SERVERS = list_servers
        aioproducer = AIOKafkaProducer(loop=loop, bootstrap_servers=BOOTSTRAP_SERVERS)
        await aioproducer.start()
    else:
        print("No hay servidores de kafka")
    try:
        print("Sending email")
        dict_email = {
            "receivers": ['me@cristdulcey.com'],
            "subject": "account confirmed",
            "body": "your account has been confirmed",
        }
        await aioproducer.send('emails', json.dumps(dict_email).encode("ascii"))
        print("Email sent")
    except Exception as error:
        print(f"error: {error}")
    finally:
        await aioproducer.stop()


loop.run_until_complete(send_email())
