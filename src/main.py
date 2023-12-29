import asyncio
from tkinter import Tk, filedialog
from schema import create_schema

import weaviate

from datas import add_batch_object, decode_image, save_as_image, save_as_json

client = weaviate.Client("http://localhost:8080")

class_obj = {
    "class": "Meme",
    "description": "A meme with similar images and text",
    "vectorizer": "img2vec-neural",
    "vectorIndexType": "hnsw",
    "moduleConfig": {
        "img2vec-neural": {
            "imageFields": [
                "image"
            ]
        }
    },
    "properties": [
        {
            "dataType": [
                "blob"
            ],
            "description": "Image of the meme.",
            "name": "image"
        },
        {
            "dataType": [
                "text"
            ],
            "description": "Name of the meme.",
            "name": "name"
        }
    ],
}


async def save_images(result):
    for i, meme in enumerate(result['data']['Get']['Meme']):
        image = meme['image']
        name = meme['name']
        await save_as_image(decode_image(image), f"{name}_{i}.jpg")


async def check_validation():
    # get datas from csv and chek if it atches te name of the label in the image
    pass


async def main():
    # print(create_schema(client, class_obj))
    # add_batch_object(client, "Meme", "img/valid.csv", batch_size=100, no_of_workers=4)
    root = Tk()
    root.withdraw()
    filename = filedialog.askopenfilename(
        initialdir="img", title="Select a file", filetypes=(("jpeg files", "*.jpg"), ("all files", "*.*")))
    nearImage = {"image": f"{filename}"}
    result = (
        client.query
        .get("Meme", ["image", "name"])
        .with_near_image(nearImage, encode=True)
        .with_limit(1)
        .do()
    )
    await save_images(result)
    await save_as_json(result, "result.json")

asyncio.run(main())
