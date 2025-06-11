import pandas as pd
from database.connect_supabase import SupabaseFunctions
from ui_setup.data_dmkt.cutting_forecast import CuttingForecast
from ui_setup.data_dmkt.get_dmsm_sql import DemandSM

class DemandTechnical():
    def __init__ (self, code_name):
        self.code_name = code_name
        self.supabase = SupabaseFunctions()

    def process_to_technical(self, code_str):
        try:
            condition = f' "GO" IN ({code_str}) '

            df_submat = self.supabase.get_data("submat_demand", ' "GO", "CODE_HQ", "TOTAL_SUB_USED" ', condition )
            df_fabric = self.supabase.get_data("cutting_forecast", ' "GO", "CODE_CUSTOMS", "TOTAL_FB_USED", "Plan_Cut_Qty" ', condition )

            df_submat = df_submat.rename(columns={"GO": "SC_NO", "CODE_HQ": "CODE_CUSTOMS", "TOTAL_SUB_USED": "TOTAL"})
            df_fabric = df_fabric.rename(columns={"GO": "SC_NO", "CODE_CUSTOMS": "CODE_CUSTOMS", "TOTAL_FB_USED": "TOTAL", "Plan_Cut_Qty": "TOTAL_PCS"})

            df_demand = pd.concat([df_submat, df_fabric], ignore_index=True)

            if df_demand.empty:
                print("❌ Không tìm thấy dữ liệu technical_demand")
                return
            
            df_demand["TOTAL"] = df_demand["TOTAL"].fillna(0)
            df_demand["CODE_CUSTOMS"] = df_demand["CODE_CUSTOMS"].fillna("")
            df_demand["TOTAL_PCS"] = df_demand["TOTAL_PCS"].fillna(0)
            
            df_group = df_demand.groupby(["SC_NO", "CODE_CUSTOMS"]).agg(TOTAL=("TOTAL", "sum"), TOTAL_PCS=("TOTAL_PCS", "sum")).reset_index()

            if self.supabase.insert_data("dm_technical", df_group.to_dict('records')) == False:
                print("❌ Lỗi khi đưa dữ liệu sang dm_technical")
                return

        except Exception as e:
            print(f"❌ Lỗi process_to_technical: {e}")

    def process_submat_demand(self):
        try:
            # Gọi hàm update trong supbase để cập nhật submat_demand
            if self.supabase.update_submat_demand() == False:
                print("❌ Lỗi khi update dữ liệu submat_demand")
                return
            
        except Exception as e:
            print(f"Lỗi khi xử lý submat demand: {e}")

    def process_fabric_demand(self):
        try:
            data_cutting_forecast = self.supabase.get_data("cutting_forecast", "*", ' "CODE_CUSTOMS" IS NULL ')

            data_fabric_list = self.supabase.get_data("fabric_list", "*")

            if data_fabric_list.empty:
                print("❌ Không có hoặc không tìm thấy dữ liệu fabric_list")
                return

            # Tạo dict PO_NO -> CODE_CUSTOMS (nối bằng dấu , nếu có nhiều)
            customs_map = (
                data_fabric_list.groupby("PO_NO")["CODE_CUSTOMS"]
                .apply(lambda x: ",".join(sorted(set(x.dropna().astype(str)))))
                .to_dict()
            )

            width_map = (
                data_fabric_list.dropna(subset=["PO_NO", "Width"])
                .groupby("PO_NO")["Width"]
                .first()
                .to_dict()
            )

            if data_cutting_forecast.empty:
                print("❌ Không có hoặc tìm thấy dữ liệu cutting_forecast mới để cập nhật")
                return
            
            # Tính CODE_CUSTOMS cho từng dòng của cutting_forecast
            data_cutting_forecast["CODE_CUSTOMS"] = data_cutting_forecast["PPO_No"].map(customs_map).fillna("")
            data_cutting_forecast["Width"] = data_cutting_forecast["PPO_No"].map(width_map).fillna(0)
            data_cutting_forecast["Marker_YY"] = data_cutting_forecast["Marker_YY"].fillna(0)
            data_cutting_forecast["Plan_Cut_Qty"] = data_cutting_forecast["Plan_Cut_Qty"].fillna(0)

            value_1 = 0.9144
            value_2 = 0.0254

            data_cutting_forecast["TOTAL_FB_USED"] = data_cutting_forecast["Marker_YY"] * data_cutting_forecast["Width"] * data_cutting_forecast["Plan_Cut_Qty"] * value_1 * value_2

            # Update to supabase
            table_name = "cutting_forecast"
            item_values = ["CODE_CUSTOMS", "Width", "TOTAL_FB_USED"]
            conditions = ["id", "PPO_No"]

            data_update = data_cutting_forecast.to_dict('records')
            update_json = [dict(t) for t in data_update]

            if self.supabase.update_batch(table_name, item_values, conditions, update_json, False):
                print(f"✅ Đã update dữ liệu fabric: {len(data_cutting_forecast)} dòng")

        except Exception as e:
            print(f"❌ Lỗi khi process_fabric_demand: {e}")

    def process_update_technical(self):
        try:
            # Update trên supabase bằng function
            if self.supabase.update_dm_technical() == False:
                print("❌ Lỗi khi update dữ liệu TOTAL PCS & DEMAND của dm_technical")
                return
            
            df_demand = self.supabase.get_data("dm_technical", "*")

            df = df_demand[(~df_demand["DEMAND"].isnull()) & (df_demand["CODE_CUSTOMS"].isin(["CA", "CB", "CST"]))]

            # Tính DEMAND.CA cho từng SC_NO
            ca = df[df['CODE_CUSTOMS'] == 'CA'][['SC_NO', 'DEMAND', 'TOTAL']]
            ca = ca.rename(columns={'DEMAND': 'DEMAND_CA', 'TOTAL': 'TOTAL_CA'})

            # Gộp DEMAND_CA và TOTAL_CA vào df theo SC_NO
            df = df.merge(ca[['SC_NO', 'DEMAND_CA', 'TOTAL_CA']], on='SC_NO', how='left')

            # Tính DEMAND mới cho CB
            mask_cb = df['CODE_CUSTOMS'] == 'CB'
            df.loc[mask_cb, 'DEMAND'] = df.loc[mask_cb, 'DEMAND_CA'] * df.loc[mask_cb, 'TOTAL'] / df.loc[mask_cb, 'TOTAL_CA']

            # Tính DEMAND mới cho CST
            mask_cst = df['CODE_CUSTOMS'].str.startswith('CST')
            df.loc[mask_cst, 'DEMAND'] = df.loc[mask_cst, 'DEMAND_CA'] * 3

            df['DEMAND'] = df['DEMAND'].fillna(0)

            # Update to supabse (dm_submat)
            data_update = df.loc[df['CODE_CUSTOMS'] != 'CA', ['SC_NO', 'CODE_CUSTOMS', 'DEMAND']].to_dict('records')
            update_json = [dict(t) for t in data_update]

            if self.supabase.update_batch("dm_technical", ["DEMAND"], ["SC_NO", "CODE_CUSTOMS"], update_json, False):
                print(f"✅ Đã update dữ liệu demand: {len(df)} dòng")

            self.update_note_check_technical()

        except Exception as e:
            print(f"❌ Lỗi khi update technical: {e}")
    
    def update_note_check_technical(self):
        try:
            # Lấy toàn bộ dữ liệu dm_technical
            df = self.supabase.get_data("dm_technical", "*")

            # Lấy dữ liệu range_dm để join
            range_dm = self.supabase.get_data("range_dm", "*")
            if range_dm.empty:
                print("❌ Không tìm thấy dữ liệu range_dm")
                return

            # Merge để lấy MIN, MAX, CODE_NAME, RANGE cho từng CODE_CUSTOMS
            df = df.merge(
                range_dm[["CODE", "MIN", "MAX", "CODE_NAME", "RANGE"]],
                left_on="CODE_CUSTOMS", right_on="CODE", how="left"
            )

            def check_note(row):
                if pd.notnull(row["DEMAND"]) and pd.notnull(row["MIN"]) and pd.notnull(row["MAX"]):
                    if row["MIN"] < row["DEMAND"] < row["MAX"]:
                        return row["CODE_NAME"]
                return None

            # Tính NOTE
            df["NOTE"] = df.apply(lambda row: check_note(row), axis=1)

            required_codes = {"CA", "CST", "IN", "THR", "PB", "W-FAB"}

            # Tạo hàm kiểm tra xuất hiện bất kỳ chuỗi nào trong required_codes
            def find_codes_in_row(codes):
                found = set()
                for req in required_codes:
                    for code in codes:
                        if pd.notnull(code) and req in str(code):
                            found.add(req)
                return found

            # Lấy các CODE_CUSTOMS thực tế xuất hiện cho từng SC_NO (theo logic mới)
            go_codes = df.groupby("SC_NO")["CODE_CUSTOMS"].apply(lambda codes: find_codes_in_row(codes))

            # Tìm các code còn thiếu
            missing_codes = go_codes.apply(lambda found: required_codes - found)

            # Tạo dict tra cứu GO thiếu CODE_CUSTOMS
            missing_dict = missing_codes.to_dict()

            def check_dm(row):
                # CHECK_DM: chỉ ghi "Không {row['RANGE']}" nếu NOTE bị thiếu
                if pd.isnull(row["NOTE"]) and row["DEMAND"] > 0:
                    return f"Không {row['RANGE']}" if pd.notnull(row["RANGE"]) else None
                
                return None
            
            def remark_dm(row, missing_dict):
                miss = missing_dict.get(row["SC_NO"], set())
                # REMARK: chỉ ghi GO thiếu hoặc thiếu nếu miss không rỗng
                if pd.isnull(row["NOTE"]) and pd.isnull(row["CHECK_DM"]):
                    return "Kiểm lại PPO_No hoặc Product_code"
                
                elif row["DEMAND"] == 0 and (row["CODE_CUSTOMS"].startswith("CB") or row["CODE_CUSTOMS"].startswith("CST")):
                    return "GO không có CA để tính CB và CST"
                
                elif miss and row["CODE_CUSTOMS"] != '':
                    return f"GO thiếu: {', '.join(sorted(miss))}"               

                return None

            df["CHECK_DM"] = df.apply(check_dm, axis=1)
            df["REMARK"] = df.apply(lambda row: remark_dm(row, missing_dict), axis=1)

            df = df.drop(columns=["id"])
            
            df = df.drop(columns=["CODE","MIN", "MAX", "CODE_NAME", "RANGE"])

            # pdate lại lên supabase
            update_cols = ["NOTE", "CHECK_DM", "REMARK"]
            key_cols = ["SC_NO", "CODE_CUSTOMS"]
            update_json = df[["SC_NO", "CODE_CUSTOMS", "NOTE", "CHECK_DM", "REMARK"]].to_dict("records")

            if self.supabase.update_batch("dm_technical", update_cols, key_cols, update_json, False):
                print(f"✅ Cập nhật và truy vấn dữ liệu Technical thành công: {len(df)} dòng")

        except Exception as e:
            print(f"❌ Lỗi khi update dữ liệu demand: {e}")

    def get_results_dm_technical(self):        
        code_str = self.code_name

        self.process_submat_demand()
        self.process_fabric_demand()

        if self.supabase.delete_data("dm_technical", f' "SC_NO" IN ({code_str}) '):
            self.process_to_technical(code_str)
            self.process_update_technical()
            return True
        else:
            print("❌ Lỗi khi xóa dữ liệu dm_technical")
            return False