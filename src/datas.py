import json
import os
import shutil

import aiofiles
import pandas as pd
from weaviate import Client
from weaviate.util import image_decoder_b64, image_encoder_b64

counter = 0
interval = 60


def add_batch_object(client: Client, class_name: str, file_name: str, batch_size: int, no_of_workers: int) -> None:
    def add_object(obj) -> None:
        global counter
        encoded_image = encode_image(
            f"img/{obj['dataset']}/{obj['label']}/{obj['image']}")
        properties = {
            'image': encoded_image,
            'name': obj['label'],
        }
        client.batch.add_data_object(
            data_object=properties,
            class_name=class_name,
        )
        print(f'Added {obj["dataset"]}/{obj["label"]}/{obj["image"]}')
        counter += 1
        if counter % interval == 0:
            print(f'Imported {counter} datasets...')

    client.batch.configure(
        batch_size=batch_size,
        num_workers=no_of_workers,
        timeout_retries=5,
        dynamic=True
    )
    with pd.read_csv(file_name, usecols=['dataset', 'label', 'image'], chunksize=1000) as csv_iterator:
        for chunk in csv_iterator:
            for _, row in chunk.iterrows():
                add_object(row)

    client.batch.flush()
    print(f'Finished importing {counter} datasets.')


def decode_image(image: str) -> bytes:
    return image_decoder_b64(image)


def encode_image(image: str) -> str:
    return image_encoder_b64(image)


async def save_as_image(data: bytes, file_name: str) -> None:
    if not os.path.exists('result'):
        os.makedirs('result')
    else:
        shutil.rmtree('result')
        os.makedirs('result')
    async with aiofiles.open(f"result/{file_name}", 'wb') as f:
        await f.write(data)
    print(f'Saved {file_name} as image.')


async def save_as_json(data: dict, file_name: str) -> None:
    if not os.path.exists('result'):
        os.makedirs('result')
    async with aiofiles.open(f"result/{file_name}", 'w') as f:
        await f.write(json.dumps(data, indent=4))
    print(f'Saved {file_name} as JSON.')
