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
#headers_of_clay = { #usa
 #   "x-clay-webhook-auth": "7fd35c803b295103e323" 
#}
headers_of_clay = { #aus
    "x-clay-webhook-auth": "53d5870039705a75b900"
}

#url_of_clay_wehook = "https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-e87962f1-f38a-4c93-a700-c8eb99682e5c"#usa
url_of_clay_webhook = "https://api.clay.com/v3/sources/webhook/pull-in-data-from-a-webhook-53d0e72f-6502-41d4-9cb9-68d34c57fbe1" #aus
# Fetch dataset items from Apify
dataset_base_url = "https://api.apify.com/v2/datasets"
PIPEDRIVE_TOKEN = "27d9e9ab8c16e564839bd2e7701cdae8df870092"
PIPEDRIVE_BASE_URL = "https://api.pipedrive.com/v1"
email_custom_code = "d3ea4f66c0020e31d8f6e9b7019004332a17c250" #custom fields in pipedrive
phone_custom_code = "e245c6f274d1c1023f3e3b3a161575f43a332f53"

CUSTOM_FIELDS = {
    # Person 1
    "person1_full_name": "06741854f635ba133020a3e707a4e6b92a2cecad",
    "person1_job_title": "f44a5919612a8e3a720e7144d668d1d2630e7dee",
    "person1_location": "9a7fa237317c5ce6535640c9f080c5f4f19e26c4",
    "person1_linkedin": "3dc35ee613649e02012925ba320a56e456baa0b1",
    "person1_work_email": "8732b372dcf333690790184258d0a9b3c9fee20b",
    "person1_phone": "3d21362c58f26e0c565677041010fb41bbde3bf1",

    # Person 2
    "person2_full_name": "66b796a45695e9dbdd50dd8392905e5e0aa01861",
    "person2_job_title": "0c0e4fd1738d92f00485521db8f4d9486f3fe7a6",
    "person2_location": "5fcebc80bcc2393f48b6207982d70ad3c7968e69",
    "person2_linkedin": "4970a94767d1ee1dad1fc97a9d6b972e736fdb61",
    "person2_phone": "353c5ef54c699b1959ae32c9ce7fd2a4b8abb5ab",
    "person2_work_email": "25cf48c981ab9ab316519347a6835ece494d2c93"
}

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
    async with session.post(url_of_clay_webhook, headers=headers_of_clay, json=payload_clay) as res:
        print("Status:", res.status)

# Remove duplicates and upload to Clay
async def check_uniqueness_and_send_to_clay(session, dataset_items):
    seen = set()
    unique_items = []
    for item in dataset_items:
        uniqueness = (item.get('companyName'), item.get('location'))
        if uniqueness and uniqueness not in seen:
            seen.add(uniqueness)
            unique_items.append(item)
            await send_to_clay(session, item)
    return unique_items





async def create_organization(name,  website,address):
    url = f"{PIPEDRIVE_BASE_URL}/organizations?api_token={PIPEDRIVE_TOKEN}"
    payload = {
        "name": name,
        "website": website,
        "address": address
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            print("Create Org Response:", data)  # Debug log
            return data.get("data")  # Can be None if API failed

async def create_deal(title, org_id):
    url = f"{PIPEDRIVE_BASE_URL}/deals?api_token={PIPEDRIVE_TOKEN}"
    payload = {
        "title": title,
        "organization_id": org_id
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            print("Create Deal Response:", data)
            return data.get("data", {})



async def get_all_organizations():
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{PIPEDRIVE_BASE_URL}/organizations?api_token={PIPEDRIVE_TOKEN}") as resp:
            data = await resp.json()
            return data.get("data", [])



async def update_org_fields(org_id, fields):
    async with aiohttp.ClientSession() as session:
        async with session.put(f"{PIPEDRIVE_BASE_URL}/organizations/{org_id}?api_token={PIPEDRIVE_TOKEN}", json=fields) as resp:
            return await resp.json()


@app.post("/clay")
async def receive_from_clay(request: Request):
    body = await request.json()
    
    company_name = body.get("Company-Name")
    title= company_name + " Deal"
    website = body.get("Website")
    address = body.get("Location")
    full_name = body.get("Full Name")
    job_title = body.get("Job Title")
    location = body.get("Location-person")
    linkedin = body.get("LinkedIn Profile")
    work_email = body.get("Work Email")
    

    # 1️⃣ Find or Create Organization
    orgs = await get_all_organizations()
    org_id = None
    person_number = None

    for org in orgs:
        if org["name"].strip().lower() == company_name.strip().lower():
            org_id = org.get("id")
            # check if person1_full_name is already filled
            if org.get(CUSTOM_FIELDS["person1_full_name"]):
                # Fill Person 2
                update_fields = {
                    CUSTOM_FIELDS["person2_full_name"]: full_name,
                    CUSTOM_FIELDS["person2_job_title"]: job_title,
                    CUSTOM_FIELDS["person2_location"]: location,
                    CUSTOM_FIELDS["person2_linkedin"]: linkedin,
                    CUSTOM_FIELDS["person2_work_email"]: work_email
                }
                await update_org_fields(org_id, update_fields)
                person_number = 2
                break
            else:
                org_id = None
                break
            # else:
            #     # Fill Person 1
            #     update_fields = {
            #         CUSTOM_FIELDS["person1_full_name"]: full_name,
            #         CUSTOM_FIELDS["person1_job_title"]: job_title,
            #         CUSTOM_FIELDS["person1_location"]: location,
            #         CUSTOM_FIELDS["person1_linkedin"]: linkedin,
            #         CUSTOM_FIELDS["person1_work_email"]: work_email
                   
            #     }
              #  await update_org_fields(org_id, update_fields)
            #     person_number = 1
            # break

    if not org_id:
        # Create org and fill person 1
        new_org = await create_organization(company_name,  website, address)
        org_id = new_org.get("id")
        update_fields = {
            CUSTOM_FIELDS["person1_full_name"]: full_name,
            CUSTOM_FIELDS["person1_job_title"]: job_title,
            CUSTOM_FIELDS["person1_location"]: location,
            CUSTOM_FIELDS["person1_linkedin"]: linkedin,
            CUSTOM_FIELDS["person1_work_email"]: work_email
        }
        await update_org_fields(org_id, update_fields)
        person_number = 1
        deal = await create_deal(title, org_id)

    

    return {
        "message": "Row processed",
        "org_id": org_id,
        "person_number": person_number
    }





# Webhook endpoint
@app.post("/apify")
async def handle(request: Request):
    body = await request.json()
    dataset_id = body.get("datasetId")

    if not dataset_id:
        return {"error": "Missing datasetId in request"}

    dataset_url = f"{dataset_base_url}/{dataset_id}/items"

    async with aiohttp.ClientSession() as session:
        dataset_items = await fetch(session, dataset_url)
        unique_items = await check_uniqueness_and_send_to_clay(session, dataset_items)
        return {"status": "Processed successfully"}


@app.post("/clay/update_num")
async def update_org_number(request: Request):
    body = await request.json()
    org_id = body.get("org_id")
    number = body.get("Phone-number")
    person= body.get("person_number", 1) 

    if not org_id or not number or not person:
        return {"error": "org_id , number and person are required"}

    
    url = f"{PIPEDRIVE_BASE_URL}/organizations/{org_id}?api_token={PIPEDRIVE_TOKEN}"
    if person == 1:
        payload = {
            CUSTOM_FIELDS["person1_phone"]: number
        }
    else:
        payload = {
            CUSTOM_FIELDS["person2_phone"]: number
        }

    async with aiohttp.ClientSession() as session:
        async with session.put(url, json=payload) as resp: 
            data = await resp.json()
            print(" Update Phone Response:", data)
            if "data" in data and data["data"]:
                return {"message": "Phone number updated successfully ", "org_id": org_id}
            else:
                return {"error": "Failed to update phone number", "details": data}



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
