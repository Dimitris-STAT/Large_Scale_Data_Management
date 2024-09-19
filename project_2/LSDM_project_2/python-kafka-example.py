import json
import asyncio
import pandas as pd
import numpy as np
import time, random, uuid

from aiokafka import AIOKafkaProducer

from faker import Faker


# Create a Faker instance
fake = Faker()

topic = 'test'

def serializer(value):
    return json.dumps(value).encode()

# Generate 19 random names using faker
names_list = [fake.name() for _ in range(19)]
# Add my name hardcoded
names_list.append("Dimitris Stathopoulos")

async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:29092',
        value_serializer=serializer,
        compression_type="gzip")
    # Read the spotify-songs.csv file
    songs_df = pd.read_csv("spotify-songs.csv")
    num_people, data_list = 20, []

    await producer.start()
    for id, name in enumerate(names_list):

        # Get a random index
        random_index = np.random.randint(0, len(songs_df))

        # Select a random value fromt he 'name' from songs_df
        random_song_name = songs_df.loc[random_index, 'name']

        # Generate a UUID for each row
        id = str(uuid.uuid4())

        # Create a list of dict as elements
        message = {"id": id, "person_name": name, "song": random_song_name}
        # Send the message
        await producer.send(topic, message)
        # Flush the producer to ensure the message is sent immediately
        await producer.flush()
        # Sleep for a while before sending the next message
        time.sleep(0.3)

    # Stop the producer
    await producer.stop()


while True:
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(produce())