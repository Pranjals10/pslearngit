import os, msal, requests
from sys import path
path.append('\\Program Files\\Microsoft.NET\\ADOMD.NET\\160')
from pyadomd import Pyadomd

TENANT_ID = "e4e34038-ea1f-4882-b6e8-ccd776459ca0"
CLIENT_ID = "e94e0ce4-0d03-4894-b12f-44285fbc3daf"
CLIENT_SECRET = "Ktq8Q~8u35mR2jMNMpuZleaUg9YGrqYIVwsoFaTf"
WORKSPACE_ID = "dce66871-77a2-481d-926e-d40d3ce5e182"
REPORT_ID = "1702df49-8186-4688-9b50-c488ed5985d0"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]

app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
)

token = app.acquire_token_for_client(scopes=SCOPE)
ACCESS_TOKEN = token["access_token"]
headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
workspaces = requests.get(
    "https://api.powerbi.com/v1.0/myorg/groups",
    headers=headers
).json()["value"]

report_map=[]
for ws in workspaces:
    reports=requests.get(
        f"https://api.powerbi.com/v1.0/myorg/groups/{ws['id']}/reports",
        headers=headers
    ).json()["value"]
    for r in reports:
        report_map.append((ws["name"], r["name"], r["datasetId"]))

print(report_map)
XMLA_ENDPOINT = "powerbi://api.powerbi.com/v1.0/myorg/Platform_Observability"

def extract_kpis(dataset):
    conn = f"Provider=MSOLAP;Data Source={XMLA_ENDPOINT};Catalog={dataset};"
    q = "SELECT MEASURE_NAME, EXPRESSION FROM $SYSTEM.MDSCHEMA_MEASURES"
    with Pyadomd(conn) as c:
        cur=c.cursor()
        cur.execute(q)
        return cur.fetchall()

for ws,rep,ds in report_map:
    print(ws, rep, extract_kpis(ds))
