import flet as ft
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from bs4 import BeautifulSoup
from selenium.webdriver.edge.options import Options

from database.connect_supabase import SupabaseFunctions

class CuttingForecast:
    def __init__(self, code_name):
        self.supa_func = SupabaseFunctions()
        self.code_name = code_name
    
    def get_data_web(self):
        try:
            
            code_str = self.code_name
            
            # ========== Lấy danh sách GO từ Supabase ==========
            if self.supa_func.delete_data("cutting_forecast", f' "GO" IN ({code_str}) OR "JO" IN ({code_str}) ') == True:
                print("✅ Đã xóa dữ liệu cutting forecast")
            else:
                print("❌ Lỗi khi xóa dữ liệu cutting forecast")
                return
            
            data = []
            data_remaining = []
            # ========== Lặp qua từng GO ==========
            for idx, input_go in enumerate(code_str.replace("'", "").split(",")):
                options = Options()
                options.add_argument('--headless')  # Ẩn trình duyệt
                options.add_argument('--disable-gpu')
                options.add_argument('--log-level=3')
                options.add_experimental_option('excludeSwitches', ['enable-logging'])

                # ========== Truy cập Web & Lấy HTML ==========
                driver = webdriver.Edge(options=options)
                driver.get("http://192.168.155.16/MesReports/Reports/CuttingForecast.aspx?site=EHV")

                # Nhập giá trị tìm kiếm
                driver.find_element(By.ID, "txtGO").clear()
                driver.find_element(By.ID, "txtGO").send_keys(input_go)
                driver.find_element(By.ID, "btnQuery").click()

                # Chờ web tải dữ liệu
                time.sleep(3)

                # Lấy source HTML sau khi web load xong
                html = driver.page_source
                driver.quit()

                # ========== Phân tích HTML để lấy bảng ==========
                soup = BeautifulSoup(html, 'html.parser')

                # Lấy bảng thứ 2 (theo yêu cầu)
                tables = soup.find_all("table", class_="ThinBorderTable")
                if len(tables) < 3:
                    data_remaining.append({"SC_NO":input_go})
                    print(f"❌ Không tìm thấy bảng dữ liệu cho GO: {input_go}")
                    continue

                target_table = tables[2]  # Bảng chứa dữ liệu chi tiết

                rows = target_table.find_all("tr")[1:]  # Bỏ dòng header
                
                for row in rows:
                    cols = [td.text.strip() for td in row.find_all("td")]
                    if len(cols) >= 8:
                        data.append({
                            "GO": input_go,
                            "JO": cols[0],
                            "Color": cols[1],
                            "Color_Desc": cols[2],
                            "Order_QTY": float(cols[3]),
                            "Per_OVER-Short_Allowed": str(cols[4]),
                            "Over_Short_Per": str(cols[5]),
                            "OverShort_QTY": float(cols[6]),
                            "Plan_Cut_Qty": float(cols[7]),
                            "PPO_No": cols[-3],
                            "Marker_YY": float(cols[-2]),
                            "PPO_YY": float(cols[-1])
                        })
            
            df = pd.DataFrame(data_remaining)
            if not df.empty:
                df.to_excel("GO_remaining.xlsx", index=False)
            
            data_web = pd.DataFrame(data)

            return data_web
        except Exception as e:
            print(f"❌ Lỗi khi lấy dữ liệu từ web: {e}")
            return pd.DataFrame()

    def into_supabase(self):
        data = self.get_data_web()
        if data.empty:
            print("❌ Không có dữ liệu để chèn vào Supabase")
            return
        
        # Chuyển đổi dữ liệu thành định dạng JSON
        data_json = data.to_dict('records')

        # Giả sử bạn đã có hàm insert_data để chèn dữ liệu vào Supabase
        if self.supa_func.insert_data("cutting_forecast", data_json) == True:
            print(f"✅ Đã lấy dữ liệu cutting forecast: {len(data)} dòng")
            return True
        else:
            print("❌ Lỗi khi lấy dữ liệu cutting forecast")
            return False