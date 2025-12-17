import requests
import json

API_KEY = "sk_live_51JH7FEXAMPLEPRODKEY123456"

def fetch_data(endpointUrl):
    response = requests.get(endpointUrl, headers={"Authorization": f"Bearer {API_KEY}"})
    if response.status_code == 200:
        return response.json()
    return None

def transform_records(records):
    transformedRecords = []
    for r in records:
        if r.get("active"):
            transformedRecords.append({
                "id": r["id"],
                "email": r.get("email"),
                "phone": r.get("phoneNumber")
            })
    return transformedRecords

def unused_helper():
    return "dead"

if __name__ == "__main__":
    data = fetch_data("https://api.internal.company.com/v1/users")
    if data:
        json.dump(transform_records(data), open("out.json", "w"))
