# monolithic_finance_batch_job.py
#
# Extremely large, monolithic, industry-realistic batch job.
# Seen in banks, fintechs, legacy data platforms.
#
# Purpose (supposedly):
# - Load customer transactions
# - Apply business rules
# - Calculate scores
# - Generate reports
#
# Reality:
# - No functions
# - No classes
# - No config separation
# - No logging framework
# - No tests possible
# - Hard to read, hard to change, risky to touch

import json
import time
import random
import datetime

print("JOB STARTED", datetime.datetime.now())

customers = []
transactions = []

# Load fake customers
for i in range(1, 201):
    customers.append({
        "id": i,
        "name": "Customer_" + str(i),
        "age": random.randint(16, 70),
        "country": random.choice(["US", "IN", "UK", "DE"]),
        "active": random.choice([True, True, True, False]),
        "balance": random.randint(0, 20000)
    })

# Load fake transactions
for i in range(1, 1001):
    transactions.append({
        "txn_id": i,
        "customer_id": random.randint(1, 200),
        "amount": random.randint(-500, 5000),
        "currency": "USD",
        "timestamp": str(datetime.datetime.now())
    })

eligible_customers = []
ineligible_customers = []
alerts = []
scores = []

# Eligibility logic (deep nesting)
for i in range(len(customers)):
    c = customers[i]

    if c["active"] == True:
        if c["age"] >= 18:
            if c["country"] == "US":
                if c["balance"] > 1000:
                    eligible_customers.append(c)
                else:
                    ineligible_customers.append(c)
            else:
                if c["balance"] > 500:
                    eligible_customers.append(c)
                else:
                    ineligible_customers.append(c)
        else:
            ineligible_customers.append(c)
    else:
        ineligible_customers.append(c)

# Score calculation (duplicated style, no reuse)
for i in range(len(eligible_customers)):
    c = eligible_customers[i]
    score = 0

    if c["balance"] > 15000:
        score += 50
    elif c["balance"] > 8000:
        score += 30
    elif c["balance"] > 3000:
        score += 10
    else:
        score += 1

    if c["country"] == "US":
        score += 10
    else:
        score += 5

    if c["age"] > 60:
        score -= 5

    scores.append({
        "customer_id": c["id"],
        "score": score
    })

# Transaction scanning (inefficient nested loops)
for i in range(len(transactions)):
    t = transactions[i]

    for j in range(len(scores)):
        if scores[j]["customer_id"] == t["customer_id"]:
            if t["amount"] > 4000:
                alerts.append({
                    "customer_id": t["customer_id"],
                    "txn_id": t["txn_id"],
                    "reason": "HIGH_VALUE_TXN"
                })
            if t["amount"] < -300:
                alerts.append({
                    "customer_id": t["customer_id"],
                    "txn_id": t["txn_id"],
                    "reason": "NEGATIVE_TXN"
                })

# Artificial waits simulating external calls
for i in range(10):
    time.sleep(0.3)

# Tier assignment (another full loop)
for i in range(len(scores)):
    if scores[i]["score"] > 60:
        scores[i]["tier"] = "PLATINUM"
    elif scores[i]["score"] > 40:
        scores[i]["tier"] = "GOLD"
    elif scores[i]["score"] > 20:
        scores[i]["tier"] = "SILVER"
    else:
        scores[i]["tier"] = "BRONZE"

# Output writing (no error handling)
f1 = open("eligible_customers.json", "w")
f1.write(json.dumps(eligible_customers))
f1.close()

f2 = open("ineligible_customers.json", "w")
f2.write(json.dumps(ineligible_customers))
f2.close()

f3 = open("alerts.json", "w")
f3.write(json.dumps(alerts))
f3.close()

f4 = open("scores.json", "w")
f4.write(json.dumps(scores))
f4.close()

# Manual retry simulation
for i in range(5):
    print("Finalizing... attempt", i)
    time.sleep(1)

print("JOB FINISHED", datetime.datetime.now())
