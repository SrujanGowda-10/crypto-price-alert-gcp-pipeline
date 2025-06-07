import json
import requests
from datetime import datetime
from google.cloud import pubsub_v1

# Initialize the Pub/Sub client and topic path
# PubSub client
project_id = 'e-object-459802-s8'
topic_id = 'crypto-coins-topic'
publisher_client = pubsub_v1.PublisherClient()
topic_path = publisher_client.topic_path(project_id, topic_id)

def get_crypto_data(request):
    coins = ["bitcoin", "ethereum", "solana"]
    message_ids = []

    try:
        for coin in coins:
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd"
            res = requests.get(url)
            coin_data = res.json()

            msg_data = {
                'coin': coin,
                'price_in_usd': coin_data[coin]['usd'],
                'updated_at': datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            }

            future = publisher_client.publish(
                topic_path,
                data=json.dumps(msg_data).encode('utf-8')
            )
            message_ids.append(future.result())

        return f"Data sent to topic with message IDs: {message_ids}", 200

    except Exception as e:
        return f"Error while publishing: {e}", 500
