import traceback
import pandas as pd
from settings.config import SUPABASE_API, SUPABASE_URL
from supabase import Client, create_client
print(SUPABASE_URL, SUPABASE_API)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API)

class SupabaseFunctions:
    # FUNCTION RUN ON PYTHON
    def get_data(self, table_name: str, items: str, conditions: str = None):
        try:
            res = supabase.rpc("select_data", {
                "table_name": table_name,
                "select_item": items,
                "conditions": conditions
            }).execute()

            if res:
                data = res.data
                return pd.DataFrame(data)
            
            else:
                return pd.DataFrame()
        
        except Exception as e:
            print(e)
            return pd.DataFrame()

    def update_data(self, table_name: str, set_value: str, conditions: str):
        try:
            response = supabase.rpc('update_data',
                                {'table_name': table_name, 
                                'set_value': set_value, 
                                'conditions': conditions}
                                ).execute()
            if response:
                return True
        except Exception as e:
            print(traceback.format_exc())
            return False
        
    def update_batch(self, table_name: str, set_columns: str, where_columns: str, updates: str, batch_mode: str = False):
        try:
            response = supabase.rpc('update_dynamic_batch',
                                {'table_name': table_name, 
                                'set_columns': set_columns, 
                                'where_columns': where_columns,
                                'updates': updates,
                                'batch_mode': batch_mode
                                }
                                ).execute()
            if response:
                return True
        except Exception as e:
            print(traceback.format_exc())
            return False
        
    def insert_data(self, table_name, data_json):
        try:
            response = supabase.table(table_name).insert(data_json).execute()
            if response:
                return True
        except Exception as e:
            print(traceback.format_exc())
            return False

    def truncate_table(self, table_name):
        try:
            response = supabase.rpc('truncate_func', {'table_name': table_name}).execute()
            if response:
                return True
        except Exception as e:
            print(traceback.format_exc())
            return False

    def delete_data(self, table_name, conditions = None):
        try:
            response = supabase.rpc('delete_data', {'table_name': table_name, 'conditions': conditions}).execute()
            if response:
                return True
        except Exception as e:
            print(traceback.format_exc())
            return False

    # FUNCTION RUN ON SUPABASE
    def update_submat_demand(self):
        try:
            response = supabase.rpc('update_submat_demand').execute()
            if response:
                return True
        except Exception as e:
            print(traceback.format_exc())
            return False
        
    def update_check_technical(self):
        try:
            response = supabase.rpc('update_check_technical').execute()
            if response:
                return True
        except Exception as e:
            print(traceback.format_exc())
            return False

    def insert_update_dm_technical(self):
        try:
            response = supabase.rpc('insert_update_dm_technical').execute()
            if response:
                return True
        except Exception as e:
            print(traceback.format_exc())
            return False
        
    def update_dm_technical(self):
        try:
            response = supabase.rpc('update_dm_technical').execute()
            if response:
                return True
        except Exception as e:
            print(traceback.format_exc())
            return False
        
    # FUNCTION FOR LOGIN/ REGISTER
    def get_user_by_username(self, username):
        return supabase.table("users").select("*").eq("username", username).execute()
    
    def create_user(self, username, password, role="user"):
        return supabase.table("users").insert({
            "username": username,
            "password": password,
            "role": role
        }).execute()