import pandas as pd
from database.connect_supabase import SupabaseFunctions
class ReportCompare:
    def __init__(self, code_name = None):
        self.queries = SupabaseFunctions()
        self.code_name = code_name

    def process_data(self):
        try:
            if self.code_name is not None:
                normalized_codes = []
                for code in self.code_name.split(","):
                    if len(code) > 9:
                        normalized_codes.append(f"S{code[:8]}")
                    else:
                        normalized_codes.append(code)
                
                code_str = ",".join([f"'{code}'" for code in normalized_codes])
                condition = f'"SC_NO" IN ({code_str})'
                data_dmkt = self.queries.get_data("dm_technical", "*", condition)
                data_dmtt = self.queries.get_data("dm_actual", "*", condition)
            
            else:
                data_dmkt = self.queries.get_data("dm_technical", "*")
                data_dmtt = self.queries.get_data("dm_actual", "*")
            return data_dmkt, data_dmtt
        except Exception as e:
            print(f"Error processing data: {e}")
            return pd.DataFrame(), pd.DataFrame()

    def process_compare(self):
        try:
            all_data = {}
            data_technical, data_actual = self.process_data()
            data = data_technical.merge(data_actual, how='left', on=['SC_NO', 'CODE_CUSTOMS']).rename(columns = {"id_x": "id"})
            cols = ["id", "SC_NO", "CODE_CUSTOMS", "TOTAL_AT", "TOTAL_PCS_AT", "DEMAND_AT", "TOTAL", "TOTAL_PCS", "DEMAND"]
            data = data[cols]

            data["COMPARE"] = ((data["DEMAND_AT"] / data["DEMAND"])*100).where(data["DEMAND"] > 0, 0)
            data["COMPARE"] = data["COMPARE"].apply(lambda x: f"{x:.1f}%" if pd.notnull(x) else "")

            all_data["dm_technical"] = data_technical
            all_data["dm_actual"] = data_actual
            all_data["dm_compare"] = data
            
            return all_data
        except Exception as e:
            print(f"Error processing data: {e}")
            return pd.DataFrame()