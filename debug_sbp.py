import os
import logging
from dotenv import load_dotenv
from payment import create_payment

# Load env vars
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)

def test_sbp():
    print("Testing SBP payment creation...")
    user_id = 123456789
    tariff = "week"
    
    try:
        # Try creating SBP payment
        result = create_payment(user_id, tariff, payment_method_type="sbp")
        print("\nResult:")
        print(result)
        
        if not result["success"]:
            print(f"\nERROR: {result.get('error')}")
    except Exception as e:
        print(f"\nEXCEPTION: {e}")

if __name__ == "__main__":
    if not os.getenv("YOOKASSA_SHOP_ID") or not os.getenv("YOOKASSA_SECRET_KEY"):
        print("ERROR: Credentials not found in .env")
    else:
        test_sbp()
