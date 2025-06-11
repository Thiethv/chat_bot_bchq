
import pandas as pd
from database.connect_supabase import SupabaseFunctions

class DmActual:
    def __init__(self, code_name):
        self.supa_func = SupabaseFunctions()
        self.code_name = code_name

    def get_data(self):

        go_nos_str = ",".join(f"'{code}'" for code in self.code_name.split(","))

        data_fabric_trans = self.supa_func.get_data("fabric_trans", "*", f'"SC_NO" IN ({go_nos_str}) OR "JO_NO" IN ({go_nos_str})')
        if data_fabric_trans.empty:
            print("❌ Không tìm thấy dữ liệu fabric_trans")
            return
        
        data_sm_trans = self.supa_func.get_data("submat_trans", "*", f'"SC_NO" IN ({go_nos_str}) OR "JO_NO" IN ({go_nos_str})')
        if data_sm_trans.empty:
            print("❌ Không tìm thấy dữ liệu submat_trans")
            return
        
        data_wip = self.supa_func.get_data("process_wip", "*", f'"SC_NO" IN ({go_nos_str}) OR "JO_NO" IN ({go_nos_str})')
        if data_wip.empty:
            print("❌ Không tìm thấy dữ liệu process_wip")
            return
        data_wip = data_wip[data_wip["Process_Code"] == "WHS"]

        return data_fabric_trans, data_sm_trans, data_wip
    
    def process_data(self):
        data_fabric_trans, data_sm_trans, data_wip = self.get_data()

        data_fabric_group = data_fabric_trans.groupby(["SC_NO", "CODE_CUSTOMS"]).agg(TOTAL=("TOTAL", "sum")).reset_index()
        data_sm_group = data_sm_trans.groupby(["SC_NO", "CODE_CUSTOMS"]).agg(TOTAL=("TOTAL", "sum")).reset_index()
        data_wip_group = data_wip.groupby("SC_NO").agg(Wip=("Wip", "sum")).reset_index()
        data = pd.concat([data_fabric_group, data_sm_group], ignore_index=True)

        data = data.merge(data_wip_group, how="left", on="SC_NO").rename(columns={"Wip": "TOTAL_PCS_AT", "TOTAL": "TOTAL_AT"})
        for col in data.select_dtypes(include=['float64']).columns:
            data[col] = data[col].fillna(0)

        # Tìm các SC_NO chưa có dòng CST
        data["CODE_CUSTOMS"] = data["CODE_CUSTOMS"].astype(str)  # Ensure CODE_CUSTOMS is string
        sc_no_has_cst = set(data[data["CODE_CUSTOMS"].str.startswith("CST")]["SC_NO"])
        sc_no_all = set(data["SC_NO"].unique())
        sc_no_missing_cst = sc_no_all - sc_no_has_cst

        ca_rows = data[data["SC_NO"].isin(sc_no_missing_cst)]["SC_NO"].unique().tolist()

        if ca_rows:
            # Tạo dòng CST mới
            new_rows = []
            for sc_no in ca_rows:
                new_rows.append({
                    "SC_NO": sc_no,
                    "CODE_CUSTOMS": "CST",
                    "TOTAL_AT": 0,
                    "TOTAL_PCS_AT": 0
                })

            if new_rows:
                new_rows_df = pd.DataFrame(new_rows)
                # Thêm các cột còn thiếu để khớp với data
                for col in data.columns:
                    if col not in new_rows_df.columns:
                        new_rows_df[col] = 0 if data[col].dtype.kind in "fi" else ""
                # Đảm bảo thứ tự cột giống data
                new_rows_df = new_rows_df[data.columns]
                data = pd.concat([data, new_rows_df], ignore_index=True)

        data["DEMAND_AT"] = (data["TOTAL_AT"] / data["TOTAL_PCS_AT"]).where(data["TOTAL_PCS_AT"] > 0, 0)
        
        ca = data[data["CODE_CUSTOMS"] == "CA"][["SC_NO", "DEMAND_AT"]]
        ca = ca.rename(columns={"DEMAND_AT": "DEMAND_CA_AT"})

        data = data.merge(ca, how="left", on="SC_NO")

        # Cập nhật lại DEMAND_AT cho tất cả dòng CST
        mask_cst = data["CODE_CUSTOMS"].str.startswith("CST")
        data["DEMAND_CA_AT"] = data["DEMAND_CA_AT"].fillna(0)
        data.loc[mask_cst, "DEMAND_AT"] = data.loc[mask_cst, "DEMAND_CA_AT"] * 3

        data = data.drop(columns=("DEMAND_CA_AT"))

        return data

    def update_note_actual(self):
        df = self.process_data()
        if df.empty:
            print("❌ Không có dữ liệu để cập nhật")
            return
        # Lấy dữ liệu range_dm để join
        range_dm = self.supa_func.get_data("range_dm", "*")
        if range_dm.empty:
            print("❌ Không tìm thấy dữ liệu range_dm")
            return
        
        # Merge để lấy MIN, MAX, CODE_NAME, RANGE cho từng CODE_CUSTOMS
        df = df.merge(
            range_dm[["CODE", "MIN", "MAX", "CODE_NAME", "RANGE"]],
            left_on="CODE_CUSTOMS", right_on="CODE", how="left"
        )

        def check_note(row):
            if pd.notnull(row["DEMAND_AT"]) and pd.notnull(row["MIN"]) and pd.notnull(row["MAX"]):
                if row["MIN"] < row["DEMAND_AT"] < row["MAX"]:
                    return row["CODE_NAME"]
            return None

        def check_dm(row):
            # CHECK_DM: chỉ ghi "Không {row['RANGE']}" nếu NOTE bị thiếu
            if pd.isnull(row["NOTE_AT"]) and row["DEMAND_AT"] > 0:
                return f"Không {row['RANGE']}" if pd.notnull(row["RANGE"]) else None
            
            return None
        
        df["NOTE_AT"] = df.apply(lambda row: check_note(row), axis=1)
        df["CHECK_DM_AT"] = df.apply(lambda row: check_dm(row), axis=1)

        df["REMARK_AT"] = ''
        df.loc[df["CODE_CUSTOMS"].isnull() | (df["CODE_CUSTOMS"] == ""), "REMARK_AT"] = "GO không có CODE HQ"
        df.loc[df["TOTAL_PCS_AT"] == 0, "REMARK_AT"] = "GO không có số lượng WIP"

        # df = df.drop(columns=["id"])
        df = df.drop(columns=["CODE","MIN", "MAX", "CODE_NAME", "RANGE"])

        sc_nos = df["SC_NO"].unique().tolist()
        sc_nos_str = ','.join(f"'{sc_no}'" for sc_no in sc_nos)

        if self.supa_func.delete_data("dm_actual",f'"SC_NO" IN ({sc_nos_str})'):
            print("✅ Xoá dữ liệu cũ thành công")

            if self.supa_func.insert_data("dm_actual", df.to_dict(orient="records")):
                print("✅ Cập nhật dữ liệu dm_actual thành công")
                return True
            
            else:
                print("❌ Cập nhật dữ liệu dm_actual thất bại")
                return False