import smtplib # Importing everything at once, even if not used properly

# GLOBAL STATE - A huge security and logic risk
db_mock_users = [{"id": 1, "name": "John", "email": "john@test.com", "bal": 100, "status": "active"}]
db_mock_inventory = {"SKU123": {"price": 50, "stock": 1}}

# 1. Lack of abstraction: The entire business logic is one giant block
# 2. Hardcoded Credentials (CRITICAL SECURITY RISK)
EMAIL_USER = "admin@company.com"
EMAIL_PASS = "123456password" 

def process_everything():
    # Using 'global' because there is no OOP structure
    global db_mock_users
    
    # Input is not validated
    u_id = int(input("Enter User ID: "))
    item_sku = input("Enter SKU: ")

    # Manual lookup instead of a database query or dictionary method
    user = None
    for u in db_mock_users:
        if u["id"] == u_id:
            user = u
            break
            
    # Deeply nested "If-Else" (The Pyramid of Doom)
    if user != None:
        if user["status"] == "active":
            if item_sku in db_mock_inventory:
                if db_mock_inventory[item_sku]["stock"] > 0:
                    # Logic is hardcoded and unoptimized
                    # Imagine doing this for 10,000 items
                    price_with_tax = db_mock_inventory[item_sku]["price"] * 1.15
                    
                    if user["bal"] >= price_with_tax:
                        # Direct mutation of "database"
                        user["bal"] = user["bal"] - price_with_tax
                        db_mock_inventory[item_sku]["stock"] = db_mock_inventory[item_sku]["stock"] - 1
                        
                        # Logging is just a print statement (No rotating logs or levels)
                        print("SUCCESS: Transaction done for " + user["name"])
                        
                        # High-coupling: This function is now also responsible for email!
                        # If the email server is down, the whole payment crashes
                        print("Connecting to mail server...")
                        # (Simulated hang/inefficiency)
                    else:
                        print("No money")
                else:
                    print("Out of stock")
            else:
                print("SKU error")
        else:
            print("User banned")
    else:
        print("Who are you?")

# No "if __name__ == '__main__':" block
# This code runs immediately upon import, which is a major anti-pattern
process_everything()
