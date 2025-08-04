from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
import aiofiles
import json
from apify_client import ApifyClientAsync

app = FastAPI()

@app.post("/webhook")
async def handle_webhook(request: Request):
    body = await request.json()
    dataset_id = body["data"]["defaultDatasetId"]
    print(f"📦 Webhook triggered, dataset ID: {dataset_id}")

    client = ApifyClientAsync("apify_api_yourtoken")  # your token here

    results = []
    async for item in client.dataset(dataset_id).iterate_items():
        results.append(item)

    # Filter unique items based on job title + location
    seen = set()
    unique_items = []
    for item in results:
        key = (item.get("title", "").lower(), item.get("location", "").lower())
        if key not in seen:
            seen.add(key)
            unique_items.append(item)

    async with aiofiles.open("D:\Python\Arrivy\pakistan.json", "w") as f:
        await f.write(json.dumps(unique_items, indent=4, ensure_ascii=False))

    print(f"✅ Saved {len(unique_items)} unique jobs to data.json")
    return {"status": "success"}
