import traceback
import pandas as pd
from sqlalchemy import create_engine
import urllib
from settings.config import USER_NAME, PASSWORD

class ConnectSQLServer:
    def __init__(self):
        self.engine = None
        self.user = USER_NAME
        self.password = PASSWORD

        self.connectSQL()

    def connectSQL(self):
        try:
            # Thông tin kết nối
            params = urllib.parse.quote_plus(
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=esq-mssql-std-dm.cogfagymhkon.ap-southeast-2.rds.amazonaws.com;"
                "DATABASE=ESQ_DATA;"
                f"UID={USER_NAME};"
                f"PWD={PASSWORD}"
            )

            # Tạo engine SQLAlchemy
            self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
            return self.engine
        except Exception as e:
            print(f"Error: {e}")

    def getData(self, query):
        try:
            print(self.engine)
            if self.engine is not None:
                with self.engine.connect() as conn:
                    data = pd.read_sql(query, conn)
                    return data
            else:
                print("Kết nối SQL không tồn tại.")

                return pd.DataFrame()
        except Exception as e:
            traceback.print_exc()
            print(f"Lỗi khi lấy dữ liệu {e}")
            return pd.DataFrame()