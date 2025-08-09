# FASTAPI Version: Webhook Trigger --> Extract Dataset --> Send to Clay
from fastapi import FastAPI, Request
import aiohttp
import asyncio
import json
import uvicorn
import requests

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

API_TOKEN = "27d9e9ab8c16e564839bd2e7701cdae8df870092"
BASE_URL = "https://api.pipedrive.com/v1"
EMAIL_CUSTOM_FIELD = "d3ea4f66c0020e31d8f6e9b7019004332a17c250"

async def get_all_organizations():
    url = f"{BASE_URL}/organizations?api_token={API_TOKEN}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            return data.get("data", [])

async def create_organization(name, email, website, industry, address):
    url = f"{BASE_URL}/organizations?api_token={API_TOKEN}"
    payload = {
        "name": name,
        EMAIL_CUSTOM_FIELD: email,
        "website": website,
        "address": address
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            print("📌 Create Org Response:", data)  # Debug log
            return data.get("data")  # Can be None if API failed

async def create_lead(title, org_id):
    url = f"{BASE_URL}/leads?api_token={API_TOKEN}"
    payload = {
        "title": title,
        "organization_id": org_id
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            return data.get("data", {})

@app.post("/clay")
async def receive_from_clay(request: Request):
    body = await request.json()
    print(body)

    # Save incoming data locally
    try:
        with open("clay_data.json", "r") as f:
            existing_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []

    existing_data.append(body)
    with open("clay_data.json", "w") as f:
        json.dump(existing_data, f, indent=4)

    print("New row saved from Clay.")

    # Extract fields from Clay
    title = body.get("Title")
    email = body.get("Email")
    company_name = body.get("Company-Name")
    website = body.get("Website")
    industry = body.get("Sector")
    address = body.get("Location")

    # --- Pipedrive Logic ---
    orgs = await get_all_organizations()
    org_id = None

    # Check if org exists
    for org in orgs:
        if org["name"].strip().lower() == company_name.strip().lower():
            org_id = org["id"]
            break

    # Create org if not exists
    if not org_id:
        new_org = await create_organization(company_name, email, website, industry, address)
        if not new_org:
            return {"error": "Failed to create organization", "details": new_org}
        org_id = new_org["id"]

    # Create lead
    await create_lead(title, org_id)

    return {
        "message": "Row received, org & lead processed",
        "org_id": org_id
    }
@app.post("/clay/update_num")
async def update_org_number(request: Request):
    body = await request.json()
    org_id = body.get("org_id")
    number = body.get("Phone-number")

    if not org_id or not number:
        return {"error": "org_id and number are required"}

    PHONE_CUSTOM_FIELD = "e245c6f274d1c1023f3e3b3a161575f43a332f53"
    url = f"{BASE_URL}/organizations/{org_id}?api_token={API_TOKEN}"
    payload = {
        PHONE_CUSTOM_FIELD: number
    }

    async with aiohttp.ClientSession() as session:
        async with session.patch(url, json=payload) as resp:  # PATCH instead of PUT
            data = await resp.json()
            print(" Update Phone Response:", data)
            if "data" in data and data["data"]:
                return {"message": "Phone number updated successfully ", "org_id": org_id}
            else:
                return {"error": "Failed to update phone number", "details": data}

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
@app.get("/clay/data")
async def get_saved_data():
    try:
        with open("clay_data.json", "r") as f:
            data = json.load(f)
        return {"data": data}
    except FileNotFoundError:
        return {"data": []}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
