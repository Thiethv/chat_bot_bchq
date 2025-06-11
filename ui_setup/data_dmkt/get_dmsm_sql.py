import pandas as pd
from datetime import datetime

from database.connect_sqlserver import ConnectSQLServer
from database.connect_supabase import SupabaseFunctions


class DemandSM:
    def __init__(self, code_sd, code_gq):
        self.queries = ConnectSQLServer()
        self.supa_func = SupabaseFunctions()
        self.code_sd = code_sd
        self.code_gq = code_gq

    def get_data_demand(self):
        '''
        Lấy dữ liệu Demand submat từ SQL Server và đẩy lên Supabase
        Chỉ lấy dữ liệu từ 6 tháng trước tháng hiện tại
        '''
        code_str = self.code_sd

        # Tính year và from_month từ 6 tháng trước tháng hiện tại
        today = datetime.today()
        six_months_ago = today.replace(day=1)  # Đầu tháng hiện tại
        for _ in range(3):  # Thử tối đa 2 lần (6 tháng trước, rồi lùi tiếp 6 tháng nữa nếu không có dữ liệu)
            # Lùi 12 tháng
            month = six_months_ago.month - 12
            year = six_months_ago.year
            if month <= 0:
                month += 12
                year -= 1

            from_date = datetime(year, month, 1)
            
            query = f"""
                SELECT * 
                FROM dbo.V_MRP_JO_Demand_EHV
                WHERE [Required Qty] > 0 
                AND [Required Qty] IS NOT NULL
                AND [Create Date] >= '{from_date.strftime('%Y-%m-%d')}'
                AND (LEFT([JO NO], 8) IN ({code_str}) OR [JO NO] IN ({code_str}))
            """

            df_month = self.queries.getData(query)        

            if not df_month.empty:
                break  # Có dữ liệu thì dừng
            else:
                print(f"❌ Không tìm thấy dữ liệu dmsm trong 6 tháng từ {from_date.strftime('%m/%Y')}, thử lùi tiếp 12 tháng nữa...")
                six_months_ago = from_date  # Lùi tiếp 12 tháng nữa

        if df_month.empty:
            print(f"❌ Không tìm thấy dữ liệu sau khi đã lùi 3 lần 6 tháng!")
            return       
        
        df_month["GO"] = "S" + df_month['JO NO'].str[:8]

        df_all = df_month
        cols = [0, 7, 8, 9, 10, 11, 15, 17, 19, 20, 21]
        df_all = df_all.iloc[:, cols]

        df_all.columns = [
            "JO_NO",
            "Required_Qty",
            "Allocated_Qty",
            "Issued_Qty",
            "Demand_Qty",
            "UOM",
            "Manual_Demand",
            "Create_Date",
            "Product_Code",
            "Dimm_No",
            "GO"
        ]

        # --- Đẩy lên supbase ---
        if df_all.empty:
            print(f"✅ Dữ liệu đã có trong Database!")
            return
        
        df_remaining = df_all
        df_remaining = df_remaining.dropna()
        if "Create_Date" in df_remaining.columns:
            df_remaining["Create_Date"] = df_remaining["Create_Date"].dt.strftime("%Y-%m-%d %H:%M:%S")
        data_json = df_remaining.to_dict('records')

        if self.supa_func.delete_data("submat_demand", f' "JO_NO" IN ({code_str}) OR "GO" IN ({code_str}) ') == True:
            if self.supa_func.insert_data("submat_demand", data_json) == True:
                print(f"✅ Đã lấy dữ liệu submat demand so với list GO: {df_remaining['GO'].nunique()} / {code_str.count(',') + 1}")
                return True
    
    def get_go_quantity(self):
        '''
            Đã đưa lên supabase dữ liệu năm 2023, 2024 và 2025 ngày 15/5
        '''
        jo_nos_str = self.code_gq
   
        today = datetime.today()
        year = today.year
        df = pd.DataFrame()
        for i in range(2):  # Thử tối đa 2 lần (1 năm trước, rồi lùi tiếp 1 năm nữa nếu không có dữ liệu)
            year -= 1

            query = f"""
            SELECT [GO No], [Order QTY], [Year] 
            FROM escmowner.V_GO
            WHERE [Factory Code] = 'EHV'
            AND [Year] >= '{year}'
            AND [Order QTY] > 0
            AND [GO No] IN ({jo_nos_str})
            """

            df = self.queries.getData(query)
            
            if not df.empty:
                break  # Có dữ liệu thì dừng
            else:
                print(f"❌ Không tìm thấy dữ liệu trong năm {year}, thử lùi tiếp 1 năm nữa...")
                year -= 1
        
        df_remaining = df

        if df_remaining.empty:
            print(f"❌ Không tìm thấy dữ liệu")
            return
        df_remaining.columns = ["GO_No", "Order_QTY", "Year"]

        df_remaining = df_remaining.dropna()

        df_remaining['Year'] = df_remaining['Year'].astype(int).astype(str)
        df_remaining['Order_QTY'] = df_remaining['Order_QTY'].astype(int)
        if self.supa_func.delete_data("go_quantity", f' "GO_No" IN ({jo_nos_str}) '):
            if self.supa_func.insert_data("go_quantity", df_remaining.to_dict('records')):
                print(f"✅ Đã lấy dữ liệu được so với list GO: {df_remaining['GO_No'].nunique()} / {self.code_name.count(',') + 1}")