
from datetime import datetime
import pandas as pd
from database.connect_sqlserver import ConnectSQLServer
from database.connect_supabase import SupabaseFunctions

class SubmatTrans():
    def __init__(self, code_name):
        self.sql_query = ConnectSQLServer()
        self.supa_func = SupabaseFunctions()
        self.code_name = code_name

    def get_table(self):

        today = datetime.today()
        six_months_ago = today.replace(day=1)
        for _ in range(3):
            month = six_months_ago.month - 6
            year = six_months_ago.year
            if month <= 0:
                month += 12
                year -= 1

            from_date = datetime(year, month, 1)

            query = f'''
                SELECT * FROM [dbo].[V_Submat_Trans_Summary_EHV]
                WHERE [SC_NO] IN ({self.code_name}) OR [JO NO] IN ({self.code_name})
                AND [TRANS_DATE] >= '{from_date.strftime('%Y-%m-%d')}'
            '''
            data = self.sql_query.getData(query)

            if not data.empty:
                break  # Có dữ liệu thì dừng
            else:
                print(f"❌ Không tìm thấy dữ liệu trong 6 tháng từ {from_date.strftime('%m/%Y')}, thử lùi tiếp 6 tháng nữa...")
                six_months_ago = from_date  # Lùi tiếp 6 tháng nữa
        return data
    
    def process_data(self):
        data = self.get_table()
        if data.empty:
            print("❌ Không tìm thấy dữ liệu submat_trans")
            return
        
        cols = [0, 3, 4, 5, 6, 7, 15, 17, 19, 20]
        data = data.iloc[:, cols]
        data.columns = ["STORE_CODE", "SC_NO", "JO_NO", "TRANS_DATE", "TRANS_CD", "ITEM_CODE", "PRODUCT_GROUP_NAME", "PRODUCT_CLASS", "TRANS_UOM", "QTY"]
        split_item_code = data["ITEM_CODE"].str.split(".", n=1, expand=True)
        data["PRODUCT_CODE"] = split_item_code[0]
        data["SUB_CODE"] = split_item_code[1]

        data_trims_list = self.supa_func.get_data("trims_list", "*")
        if data_trims_list.empty:
            print("❌ Không tìm thấy dữ liệu trims_list")
            return
        
        data_result = data.merge(data_trims_list, how="left", left_on ="PRODUCT_CODE", right_on="THV_CODE")
        data_result = data_result.drop(columns=["id", "THV_CODE"])
        
        data_result["QTY"] = data_result["QTY"].apply(lambda x: abs(x) if pd.notnull(x) else x)
        data_result["QTY"] = data_result["QTY"].abs()
        data_result["TOTAL"] = data_result["QTY"].fillna(0) * data_result["CONVERT"].fillna(0)

        data_result["TRANS_DATE"] = data_result["TRANS_DATE"].dt.strftime("%Y-%m-%d %H:%M:%S")
        
        for col in data_result.select_dtypes(include=['object']).columns:
            data_result[col] = data_result[col].fillna('')
        for col in data_result.select_dtypes(include=['float64']).columns:
            data_result[col] = data_result[col].fillna(0)

        if self.supa_func.delete_data("submat_trans", f' "SC_NO" IN ({self.code_name}) OR "JO_NO" IN ({self.code_name}) ') == True:
            print("✅ Xóa dữ liệu submat_trans thành công")
            if self.supa_func.insert_data("submat_trans", data_result.to_dict(orient="records")) == True:
                print("✅ Thêm dữ liệu submat_trans thành công")
                return True
            else:
                print("❌ Lỗi khi thêm dữ liệu submat_trans")
                return False