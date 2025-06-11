import pandas as pd
from database.connect_supabase import SupabaseFunctions

class MasterList:
    def __init__(self, data):
        self.supabase = SupabaseFunctions()
        self.data = data
    
    def insert_list_trims_to_supabase(self):
        '''
            Lấy danh sách trims từ file excel và đẩy lên supabase
        '''
        if self.data.empty:
            print("❌ Không có dữ liệu trims")
            return

        # Lấy dữ liệu từ supabase
        data_supa = self.supabase.get_data("trims_list", "*")
        data = self.data[~self.data["THV_CODE"].isin(data_supa["THV_CODE"])] if not data_supa.empty else self.data

        if not data_supa.empty:           
            # Tạo dict ánh xạ THV_CODE -> (CODE_CUSTOMS, CONVERT) từ data_supa
            code_custom_convert_map = (
                data_supa.set_index("THV_CODE")[["CODE_CUSTOMS", "CONVERT"]]
                .to_dict(orient="index")
            )
            # Tạo 2 cột mới để so sánh
            self.data["Convet_in_data"] = self.data["THV_CODE"].map(lambda x: code_custom_convert_map.get(x, {}).get("CONVERT"))
            self.data["Code_customs_in_data"] = self.data["THV_CODE"].map(lambda x: code_custom_convert_map.get(x, {}).get("CODE_CUSTOMS"))

            # Lọc các THV_CODE có CONVERT hoặc CODE_CUSTOMS khác so với dữ liệu supabase
            df_check = self.data[
                (self.data["THV_CODE"].isin(data_supa["THV_CODE"])) &
                (
                    (self.data["CONVERT"] != self.data["Convet_in_data"]) |
                    (self.data["CODE_CUSTOMS"] != self.data["Code_customs_in_data"])
                )
            ]
            if not df_check.empty:
                codes = df_check["THV_CODE"].unique().tolist()
                conditions = f' "THV_CODE" IN {tuple(codes)} ' if len(codes) > 1 else f' "THV_CODE" = {codes[0]} '
                self.supabase.delete_data("trims_list", conditions)

                data = pd.concat([data, df_check], ignore_index=True)
                
            data = data.drop("Convet_in_data", axis=1)
            data = data.drop("Code_customs_in_data", axis=1)

        data_trims_list = data.copy()
        if data_trims_list.empty:
            print("❌ Không có dữ liệu trims")
            return

        data_trims_list["CONVERT"] = pd.to_numeric(data_trims_list["CONVERT"], errors='coerce')

        data_trims_list = data_trims_list.dropna()
        data_trims_list["CONVERT"] = data_trims_list["CONVERT"].astype(float)

        data_trims_list = data_trims_list.drop_duplicates()

        data_json = data_trims_list.to_dict('records')
        if self.supabase.insert_data("trims_list", data_json) == True:
            print(f"✅ Đã đưa dữ liệu trims: {len(data_trims_list)} dòng")
            return True
        else:
            print("❌ Lỗi khi lấy dữ liệu trims")
            return False
    
    def insert_list_fabric_to_supabase(self):
        '''
            Lấy danh sách fabric từ file excel và đẩy lên supabase
        '''
        if self.data.empty:
            print("❌ Không có dữ liệu fabric")
            return        

        # Lý dữ liệu từ supabase
        data_supa = self.supabase.get_data("fabric_list", "*")
        
        data = self.data[~self.data["PO_NO"].isin(data_supa["PO_NO"])] if not data_supa.empty else self.data

        if not data_supa.empty:
            # Tạo dict ánh xạ PO_NO -> Width từ data_supa
            code_custom_convert_map = (
                data_supa.dropna(subset=["PO_NO", "Width"])
                .groupby("PO_NO")["Width"]
                .first()
                .to_dict()
            )

            self.data["Convet_in_data"] = self.data["PO_NO"].map(code_custom_convert_map)

            df_check = self.data[(self.data["PO_NO"].isin(data_supa["PO_NO"]))
                                    & (self.data["Width"] != self.data["Convet_in_data"])] # Lọc các PO_NO có Width khác Convet_in_data
            if not df_check.empty:
                codes = df_check["PO_NO"].unique().tolist()
                conditions = f' "PO_NO" IN {tuple(codes)} ' if len(codes) > 1 else f' "PO_NO" = {codes[0]} '
                self.supabase.delete_data("fabric_list", conditions)

                data_concat = pd.concat([data, df_check], ignore_index=True)
                
            data = data_concat.drop("Convet_in_data", axis=1)

        data_fabric = data.copy()
        if data_fabric.empty:
            print("❌ Không có dữ liệu fabric")
            return

        data_fabric["Width"] = pd.to_numeric(data_fabric["Width"], errors='coerce')

        data_fabric = data_fabric.dropna()
        data_fabric["Width"] = data_fabric["Width"].astype(float)

        data_fabric = data_fabric.drop_duplicates()

        data_json = data_fabric.to_dict('records')
        if self.supabase.insert_data("fabric_list", data_json) == True:
             print(f"✅ Đã đưa dữ liệu fabric master: {len(data_fabric)} dòng")
             return True
        else:
            print("❌ Lỗi khi lấy dữ liệu fabric master")
            return False
    
    def insert_range_demand_to_supabase(self):
        '''
            Đẩy dữ liệu range định mức lên supabase
        '''
        if self.data.empty:
            print("❌ Không có dữ liệu range")
            return

        # Lý dữ liệu từ supabase
        data_supa = self.supabase.get_data("range_dm", "*")

        data = self.data[~self.data["CODE"].isin(data_supa["CODE"])] if not data_supa.empty else self.data

        # Đọc dữ liệu từ file Excel
        df = data.copy()
        df.columns = ["CODE", "MIN", "MAX", "CODE_NAME", "UNITS", "RANGE"]
        df["MIN"] = pd.to_numeric(df["MIN"], errors='coerce')
        df['MAX'] = pd.to_numeric(df['MAX'], errors='coerce')

        df = df.dropna()
        df["MIN"] = df["MIN"].astype(float)
        df['MAX'] = df['MAX'].astype(float)

        df = df.drop_duplicates()

        if df.empty:
            print("❌ Không tìm thấy dữ liệu range_dm")
            return
        data_json = df.to_dict('records')
        if self.supabase.insert_data("range_dm", data_json) == True:
            print(f"✅ Đã lấy dữ liệu range_dm: {len(df)} dòng")
            return True
        else:
            print("❌ Lỗi khi lấy dữ liệu range_dm")
            return False

    def insert_list_go(self):
        """
        Đẩy dữ liệu list_go lên supabase
        """
        if self.data.empty:
            print("❌ Không có dữ liệu list_go")
            return

        data_supabase = self.supabase.get_data("list_go", "*")

        data = self.data if data_supabase.empty else self.data[~self.data["SC_NO"].isin(data_supabase["SC_NO"])]

        data = data.dropna()
        if data.empty:
            print("❌ Không có dữ liệu list_go")
            return

        data_json = data.to_dict('records')
        if self.supabase.insert_data("list_go", data_json):
            print(f"✅ Đã lấy dữ liệu list_go: {len(data)} dòng")
            return True
        else:
            print("❌ Lỗi khi lấy dữ liệu list_go")
            return False