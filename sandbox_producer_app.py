import asyncio
import os

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import ClientSecretCredential

from dotenv import load_dotenv

async def run():
    credential = ClientSecretCredential(tenant_id=os.getenv("TENANT_ID"),
                                    client_id=os.getenv("CLIENT_ID"),
                                    client_secret=os.getenv("CLIENT_SECRET"))
    
    # Create a producer client to send messages to the event hub
    # Specify a credential that has correct role assigned to access event hub namespace and the event hub name
    producer = EventHubProducerClient(fully_qualified_namespace=os.getenv("EVENT_HUB_FQN"), 
                                      eventhub_name=os.getenv("EVENT_HUB_NAME"), credential=credential)
    async with producer:
        # Create a batch
        event_data_batch = await producer.create_batch()

        # Add events to the batch
        event_data_batch.add(EventData("First event"))
        event_data_batch.add(EventData("Second event"))
        event_data_batch.add(EventData("Third event"))

        # Send the batch of events to the event hub
        await producer.send_batch(event_data_batch)

        # Close credential when no longer needed
        await credential.close()

load_dotenv()
asyncio.run(run())