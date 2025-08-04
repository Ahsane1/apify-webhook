from fastapi import FastAPI, Request
import json
import aiofiles
from apify_client import ApifyClientAsync

app = FastAPI()

@app.post("/webhook")
async def webhook_handler(request: Request):
    body = await request.json()
    dataset_id = body["resource"]["defaultDatasetId"]

    client = ApifyClientAsync("apify_api_yUm3GvrXmoeG33CxHA1CeZWARHXaWj2EjfvM")  

    results = []
    async for item in client.dataset(dataset_id).iterate_items():
        results.append(item)

    async with aiofiles.open("data.json", "w") as f:
        await f.write(json.dumps(results, indent=2))

    print("✅ Data saved.")
    return {"status": "received"}
