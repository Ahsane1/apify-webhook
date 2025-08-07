# FASTAPI Version: Webhook Trigger --> Extract Dataset --> Send to Clay
from fastapi import FastAPI, Request
import aiohttp
import asyncio
import json
import uvicorn

# Auth headers and URLs
APIFY_TOKEN = "apify_api_yUm3GvrXmoeG33CxHA1CeZWARHXaWj2EjfvM"
header_of_apify = {
    "Authorization": f"Bearer {APIFY_TOKEN}"
}
headers_of_clay = {
    "x-clay-webhook-auth": "60fe90cfc871e510b345"
}
url_of_clay_wehook = "https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-6822471d-c118-40af-a632-217166c32541"

# Fetch dataset items from Apify
dataset_base_url = "https://api.apify.com/v2/datasets"

app = FastAPI()

async def fetch(session, url):
    async with session.get(url, headers=header_of_apify) as resp:
        return await resp.json()

# Send one item to Clay
async def send_to_clay(session, item):
    payload_clay = {
        "Title": item.get('title'),
        "Company Name": item.get('companyName'),
        "Company Url": item.get('companyUrl'),
        "Location": item.get('location'),
        "Sector": item.get('sector'),
        "Description": item.get('description'),
        "Job Url": item.get('jobUrl')
    }
    async with session.post(url_of_clay_wehook, headers=headers_of_clay, json=payload_clay) as res:
        print("Status:", res.status)

# Remove duplicates and upload to Clay
async def upload_to_clay(session, dataset_items):
    seen = set()
    unique_items = []
    for item in dataset_items:
        uniqueness = (item.get('companyName'), item.get('location'))
        if uniqueness and uniqueness not in seen:
            seen.add(uniqueness)
            unique_items.append(item)
            await send_to_clay(session, item)
    return unique_items

# Webhook endpoint
@app.post("/")
async def handle(request: Request):
    body = await request.json()
    dataset_id = body.get("datasetId")

    if not dataset_id:
        return {"error": "Missing datasetId in request"}

    dataset_url = f"{dataset_base_url}/{dataset_id}/items"

    async with aiohttp.ClientSession() as session:
        dataset_items = await fetch(session, dataset_url)
        unique_items = await upload_to_clay(session, dataset_items)
        return {"status": "Processed successfully"}
# Clay Webhook Receiver
@app.post("/clay")
async def receive_from_clay(request: Request):
    body = await request.json()
    job_url = body.get("jobUrl")

    if not job_url:
        return {"error": "jobUrl is required"}

    data = load_data()

    if any(entry["jobUrl"] == job_url for entry in data):
        return {"message": "Already received"}

    data.append(body)
    save_data(data)
@app.get("/clay/data")
async def get_saved_data():
    data = load_data()
    return {"data": data}
    print("New data received and saved.")
    return {"status": "Saved successfully"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
