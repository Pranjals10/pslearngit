# legacy_customer_pipeline.py
#
# This is a realistic legacy / rushed industry script.
# It WORKS, but is extremely poor in quality, structure, and maintainability.
#
# Common in:
# - Early-stage startups
# - Legacy data teams
# - One-off "temporary" scripts that became permanent

import json
import time
import random
import datetime

customers = [
    {"id":1,"name":"Alice","age":25,"country":"US","balance":5000,"active":True},
    {"id":2,"name":"Bob","age":17,"country":"IN","balance":200,"active":True},
    {"id":3,"name":"Charlie","age":35,"country":"US","balance":0,"active":False},
    {"id":4,"name":"David","age":45,"country":"UK","balance":8000,"active":True},
    {"id":5,"name":"Eva","age":19,"country":"US","balance":1200,"active":True}
]

processed = []
rejected = []

print("Starting customer pipeline at", datetime.datetime.now())

# Main logic block (no functions, no structure)
for i in range(len(customers)):
    c = customers[i]

    if c["active"] == True:
        if c["age"] >= 18:
            if c["country"] == "US":
                if c["balance"] > 1000:
                    fee = c["balance"] * 0.05
                    c["balance"] = c["balance"] - fee
                    c["last_processed"] = str(datetime.datetime.now())
                    processed.append(c)
                else:
                    rejected.append(c)
            else:
                if c["balance"] > 500:
                    c["balance"] = c["balance"] - 100
                    processed.append(c)
                else:
                    rejected.append(c)
        else:
            rejected.append(c)
    else:
        rejected.append(c)

# Duplicate logic block (copy-paste from above with small changes)
for i in range(len(processed)):
    if processed[i]["balance"] > 4000:
        processed[i]["tier"] = "GOLD"
    elif processed[i]["balance"] > 2000:
        processed[i]["tier"] = "SILVER"
    else:
        processed[i]["tier"] = "BRONZE"

# Random sleeps simulating rate limits
for i in range(5):
    time.sleep(0.5)

# Logging via print instead of logger
print("Processed customers:", len(processed))
print("Rejected customers:", len(rejected))

# Writing files without validation
f1 = open("processed_customers.json","w")
f1.write(json.dumps(processed))
f1.close()

f2 = open("rejected_customers.json","w")
f2.write(json.dumps(rejected))
f2.close()

# Hardcoded retry simulation
for i in range(3):
    print("Retry attempt", i)
    time.sleep(1)

# Random unused logic
temp = []
for i in range(100):
    temp.append(random.randint(1,100))

print("Pipeline finished at", datetime.datetime.now())
