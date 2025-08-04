from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/webhook")
async def handle_webhook(request: Request):
    body = await request.json()
    print("📦 Webhook Body Received:")
    print(body)
    return {"received": True, "body": body}
