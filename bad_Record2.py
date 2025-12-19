# filename: enterprise_processor_v9_final_FINAL_v2.py
# WARNING: This code is intentionally poor quality. 
# Do not use in production.

import sys
import datetime
import random
import time
import json
import smtplib # For emails
import socket  # For "manual" database pings

# --- GLOBAL SETTINGS (Major Security Risk: Hardcoded Credentials) ---
DB_HOST = "192.168.1.50"
DB_USER = "admin"
DB_PASS = "P@ssword123!" 
SECRET_KEY = "XYZ123_SECRET"
TAX_RATE = 0.0825
DISCOUNT_GOLD = 0.15
DISCOUNT_SILVER = 0.10

# --- GLOBAL DATA STORE (Memory Leak Risk: Unbounded Global Lists) ---
GLOBAL_TRANSACTION_LOG = []
USER_SESSION_CACHE = {}
PENDING_ORDERS = []

# --- POOR QUALITY: No Classes, just global state mutation ---

def init_system():
    # Violation: Using print for logging
    print("LOG: System starting at " + str(datetime.datetime.now()))
    # Violation: Manual socket ping instead of proper DB health checks
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((DB_HOST, 80))
        print("LOG: Connection to DB Server OK")
    except:
        print("LOG: DB Server unreachable, but we will try anyway...")
    finally:
        s.close()

def load_data_from_file_manual():
    # Violation: Not using 'with' statement for file handling
    # Violation: Manual string parsing instead of using a CSV/JSON library properly
    f = open("data_backup.txt", "r")
    data = f.read()
    lines = data.split("\n")
    for line in lines:
        parts = line.split(",")
        if len(parts) > 1:
            USER_SESSION_CACHE[parts[0]] = parts[1]
    f.close()
    print("LOG: Loaded " + str(len(USER_SESSION_CACHE)) + " users.")

def process_order(user_id, item_id, qty, price, region, user_type):
    global GLOBAL_TRANSACTION_LOG
    
    # --- THE PYRAMID OF DOOM (Deep Nesting) ---
    if user_id != "":
        if item_id != "":
            if qty > 0:
                if price > 0:
                    # Logic: Calculate total (Unoptimized)
                    subtotal = 0
                    for i in range(qty):
                        subtotal += price
                    
                    # Violation: Hardcoded region logic instead of a strategy pattern
                    if region == "US-EAST":
                        final_total = subtotal * (1 + TAX_RATE)
                    elif region == "US-WEST":
                        final_total = subtotal * (1 + 0.09) # Magic number
                    elif region == "EU":
                        final_total = subtotal * 1.20 # High VAT hardcoded
                    else:
                        final_total = subtotal
                    
                    # Violation: Logic duplication for user tiers
                    if user_type == "GOLD":
                        print("Applying Gold Discount")
                        final_total = final_total - (final_total * DISCOUNT_GOLD)
                    elif user_type == "SILVER":
                        print("Applying Silver Discount")
                        final_total = final_total - (final_total * DISCOUNT_SILVER)
                    
                    # Violation: Global side effects
                    order_packet = {
                        "id": random.randint(1000, 9999),
                        "user": user_id,
                        "amount": final_total,
                        "timestamp": str(datetime.datetime.now())
                    }
                    GLOBAL_TRANSACTION_LOG.append(order_packet)
                    
                    # Violation: Mixing Logic and I/O (Database & Email)
                    # Imagine this SQL query is vulnerable to Injection
                    query = "INSERT INTO orders VALUES ('" + str(order_packet['id']) + "', '" + user_id + "')"
                    print("EXECUTING SQL: " + query)
                    
                    # Logic: Simulated Email Notification (Blocking Call)
                    print("Connecting to SMTP...")
                    # time.sleep(1) # Simulated network lag
                    print("Email sent to user " + user_id)
                    
                    return True
                else:
                    print("ERROR: Price is zero")
                    return False
            else:
                print("ERROR: Qty is zero")
                return False
        else:
            print("ERROR: Item ID missing")
            return False
    else:
        print("ERROR: User ID missing")
        return False

def generate_report():
    # Violation: Highly inefficient O(n^2) or worse logic
    # Calculating totals by iterating over a global list every single time
    print("--- REVENUE REPORT ---")
    total_revenue = 0
    for entry in GLOBAL_TRANSACTION_LOG:
        # Violation: No error handling for missing keys
        total_revenue += entry["amount"]
    
    print("Total Revenue to date: " + str(total_revenue))
    
    # Violation: Re-calculating stats from scratch instead of using aggregation
    gold_count = 0
    for entry in GLOBAL_TRANSACTION_LOG:
        # Assume we have to look up the user again because we didn't store it well
        if entry["amount"] > 1000: # Arbitrary "gold" logic different from before
            gold_count += 1
    print("High Value Orders: " + str(gold_count))

def maintenance_cleanup():
    # Violation: Dangerous "Delete All" logic with no safety checks
    global GLOBAL_TRANSACTION_LOG
    if len(GLOBAL_TRANSACTION_LOG) > 100:
        print("LOG: Clearing logs to save memory...")
        # Just clearing the list loses all business data!
        GLOBAL_TRANSACTION_LOG = [] 

def sync_external_api_v1():
    # Violation: Manual HTTP-like string building instead of using 'requests' library
    print("Syncing to External API...")
    payload = "AUTH=" + SECRET_KEY + "&DATA=" + str(GLOBAL_TRANSACTION_LOG)
    # This would fail in reality, but it's how "bad" code tries to do it
    print("SENDING PAYLOAD: " + payload)

def main_loop():
    # Violation: Infinite loop with no exit condition or signal handling
    while True:
        print("\n--- ENTERPRISE MANAGER ---")
        print("1. New Order")
        print("2. Run Report")
        print("3. System Sync")
        print("4. Exit")
        
        choice = input("Select Option: ")
        
        if choice == "1":
            # Violation: No input sanitization
            uid = input("User ID: ")
            pid = input("Product ID: ")
            q = int(input("Quantity: "))
            p = float(input("Price: "))
            reg = input("Region: ")
            utype = input("User Type: ")
            process_order(uid, pid, q, p, reg, utype)
        elif choice == "2":
            generate_report()
        elif choice == "3":
            sync_external_api_v1()
            maintenance_cleanup()
        elif choice == "4":
            # Violation: Using sys.exit() in the middle of a script
            sys.exit()
        else:
            # Violation: Vague error handling
            print("Invalid input. Try again.")

# --- DUPLICATED CODE (Violating DRY) ---
# Someone copy-pasted the order logic for a "Bulk" version instead of using parameters
def process_bulk_order_COPY(user_id, item_id, qty, price, region, user_type):
    global GLOBAL_TRANSACTION_LOG
    if user_id != "":
        if item_id != "":
            subtotal = price * qty
            # Wait, the tax logic is different here! (Consistency bug)
            final_total = subtotal * 1.05 
            order_packet = {"id": random.randint(1,9), "amount": final_total}
            GLOBAL_TRANSACTION_LOG.append(order_packet)
            print("Bulk order processed.")

# --- STARTUP ---
init_system()
load_data_from_file_manual()

# --- MORE DEAD CODE ---
# Functions that are never called but stay in the file for years
def old_unused_tax_calc(a, b):
    return a * b / 100

def legacy_user_check(u):
    if u == "admin": return True
    return False

# Main Execution
if __name__ == "__main__":
    try:
        main_loop()
    except Exception as e:
        # Violation: Catching all exceptions and not logging the stack trace
        print("Something went wrong: " + str(e))

# ... (Imagine another 100 lines of similar repeated functions for "Refunds", 
# "Inventory Adjustments", and "Monthly Audits", all using the same global variables)
