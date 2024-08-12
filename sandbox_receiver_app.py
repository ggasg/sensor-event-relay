import asyncio
import os

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from azure.identity.aio import ClientSecretCredential

from dotenv import load_dotenv

async def on_event(partition_context, event):
    # Print event data
    print(
        'Received event: "{}" from the partition with ID: "{}"'.format(
            event.body_as_str(encoding="UTF-8"), partition_context.partition_id
        )
    )
    # Update the checkpoint so that the program doesnt re-read events next time
    await partition_context.update_checkpoint(event)

async def main():
    credential = ClientSecretCredential(tenant_id=os.getenv("TENANT_ID"),
                                    client_id=os.getenv("CLIENT_ID"),
                                    client_secret=os.getenv("CLIENT_SECRET"))
    
    # Leverage azure blob checkpoint store to store the checkpoints
    checkpoint_store = BlobCheckpointStore(
        blob_account_url=os.getenv("BLOB_STORAGE_ACCOUNT_URL"),
        container_name=os.getenv("BLOB_CONTAINER_NAME"),
        credential=credential
    )

    # Create the consumer client for the event hub
    client = EventHubConsumerClient(
        fully_qualified_namespace=os.getenv("EVENT_HUB_FQN"),
        eventhub_name=os.getenv("EVENT_HUB_NAME"),
        consumer_group="$Default",
        checkpoint_store=checkpoint_store,
        credential=credential
    )
    async with client:
        # Read from the beginning of the partition
        # (starting_position: "-1")
        await client.receive(on_event=on_event, starting_position="-1")

        # Close credential
        await credential.close()

load_dotenv()
asyncio.run(main())