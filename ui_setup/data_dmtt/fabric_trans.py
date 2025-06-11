
from datetime import datetime
import pandas as pd
from database.connect_sqlserver import ConnectSQLServer
from database.connect_supabase import SupabaseFunctions


class FabricTrans():
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
                SELECT * FROM [dbo].[V_Fabric_Trans_Summary_EHV]
                WHERE ([SC_NO] IN ({self.code_name}) OR [JO NO] IN ({self.code_name}))
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
            print("❌ Không tìm thấy dữ liệu fabric_trans")
            return
        cols = [3, 4, 5, 6, 7, 8, 9, 19, 20]
        data = data.iloc[:, cols]
        data.columns = ["SC_NO", "JO NO", "TRANS_DATE", "TRANS_CD", "ITEM_CODE", "PO_NO", "TRANS TYPE", "TRANS_UOM", "QTY"]
        data["PO_Item"] = data["PO_NO"] + " " + data["ITEM_CODE"]

        # Thêm các cột CUSTOMS CODE, Width, Total
        data_fabric_supbase = self.supa_func.get_data("fabric_list", "*")
        if data_fabric_supbase.empty:
            print("❌ Không tìm thấy dữ liệu fabric_list")
            return
        data_result = data.merge(data_fabric_supbase, how="left", on="PO_Item").rename(columns={
            "PO_NO_x": "PO_NO", "JO NO": "JO_NO", "TRANS TYPE":"TRANS_TYPE"})
        data_result = data_result.drop(columns=["PO_NO_y", "id"])

        for col in data_result.select_dtypes(include=['object']).columns:
            data_result[col] = data_result[col].fillna('')

        for col in data_result.select_dtypes(include=['float64']).columns:
            data_result[col] = data_result[col].fillna(0)

        data_result["QTY"] = data_result["QTY"].apply(lambda x: abs(x) if pd.notnull(x) else x)
        data_result["QTY"] = data_result["QTY"].abs()
        data_result["TOTAL"] = data_result["QTY"].fillna(0) * data_result["Width"].fillna(0) * 0.9144 * 0.0254

        data_result["TRANS_DATE"] = data_result["TRANS_DATE"].dt.strftime("%Y-%m-%d %H:%M:%S")        

        if self.supa_func.delete_data("fabric_trans", f' "SC_NO" IN ({self.code_name}) OR "JO_NO" IN ({self.code_name})') == True:
            print("✅ Xóa dữ liệu fabric_trans thành công")

            if self.supa_func.insert_data("fabric_trans", data_result.to_dict(orient="records")) == True:
                print("✅ Thêm dữ liệu fabric_trans thành công")
                return True
            else:
                print("❌ Lỗi khi thêm dữ liệu fabric_trans")
                return False
            