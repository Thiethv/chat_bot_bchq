import os
from dotenv import load_dotenv

load_dotenv()  # Chỉ dùng cho local

USER_NAME = os.getenv("USER_NAME")
PASSWORD = os.getenv("PASSWORD")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API = os.getenv("SUPABASE_API")
