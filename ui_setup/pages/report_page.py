import io
from typing import List
import flet as ft
import pandas as pd
from database.connect_supabase import SupabaseFunctions
from datetime import datetime
import uuid
import unicodedata

import re
import qdrant_client
from qdrant_client.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer

# --- Qdrant setup ---
collection_name = "command_embeddings"
embedding_model = SentenceTransformer("thenlper/gte-base")
qdrant = qdrant_client.QdrantClient(path="./qdrant_data")

def init_collection():
    if collection_name not in [c.name for c in qdrant.get_collections().collections]:
        qdrant.recreate_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=768, distance=Distance.COSINE),
        )

RULE_KEYWORDS = {
    "process wip": ("option10", "JO Process Wip"),
    "dm actual": ("option8", "Report Actual"),
    "dm technical": ("option1", "Report Technical"),
    "submat demand": ("option5", "Submat Demand"),
    "compare": ("option12", "Report Compare"),
    "cutting forecast": ("option3", "Cutting Forecast"),
    "go quantity": ("option4", "Go Quantity"),
    "list go": ("option2", "List GO"),
    "submat trans": ("option11", "Submat Trans Summary"),
    "fabric trans": ("option9", "Fabric Trans Summary"),
    "master fabric": ("option6", "Master Fabric List"),
    "master trims": ("option7", "Master Trims List"),
}



sample_data = [
    # option1 - DM Technical
    {"id": 1, "command": "option1", "desc": "Báo cáo kỹ thuật Demand Technical"},
    {"id": 1, "command": "option1", "desc": "Báo cáo DM Technical"},
    {"id": 1, "command": "option1", "desc": "Xem DM Technical"},
    {"id": 1, "command": "option1", "desc": "Tôi muốn xem dữ liệu kỹ thuật"},
    {"id": 1, "command": "option1", "desc": "Hiển thị dữ liệu kỹ thuật của SC1234"},
    {"id": 1, "command": "option1", "desc": "Demand kỹ thuật là gì?"},

    # option2 - List GO
    {"id": 2, "command": "option2", "desc": "Danh sách GO"},
    {"id": 2, "command": "option2", "desc": "List GO"},
    {"id": 2, "command": "option2", "desc": "Liệt kê các GO"},
    {"id": 2, "command": "option2", "desc": "Cho tôi danh sách mã GO"},
    {"id": 2, "command": "option2", "desc": "Tất cả các mã GO đang có"},

    # option3 - Cutting Forecast
    {"id": 3, "command": "option3", "desc": "Dự báo Cutting Forecast"},
    {"id": 3, "command": "option3", "desc": "Lấy dữ liệu Cutting Forecast"},
    {"id": 3, "command": "option3", "desc": "Dự báo cắt vải"},
    {"id": 3, "command": "option3", "desc": "Cutting forecast cho SC1234"},

    # option4 - GO Quantity
    {"id": 4, "command": "option4", "desc": "Số lượng của GO"},
    {"id": 4, "command": "option4", "desc": "GO Quantity"},
    {"id": 4, "command": "option4", "desc": "Số lượng sản xuất theo GO"},
    {"id": 4, "command": "option4", "desc": "Có bao nhiêu hàng cho mã GO này?"},

    # option5 - Submat Demand
    {"id": 5, "command": "option5", "desc": "Báo cáo Submat Demand"},
    {"id": 5, "command": "option5", "desc": "Báo cáo Demand Phụ liệu"},
    {"id": 5, "command": "option5", "desc": "Xem nhu cầu phụ liệu"},
    {"id": 5, "command": "option5", "desc": "Submat demand là gì?"},
    {"id": 5, "command": "option5", "desc": "Cần phụ liệu gì cho GO này?"},

    # option6 - Master Fabric List
    {"id": 6, "command": "option6", "desc": "Danh sách Master Fabric"},
    {"id": 6, "command": "option6", "desc": "List Master Fabric"},
    {"id": 6, "command": "option6", "desc": "Danh sách vải chính"},
    {"id": 6, "command": "option6", "desc": "Fabric list đang dùng"},

    # option7 - Master Trims List
    {"id": 7, "command": "option7", "desc": "Danh sách Master Trims"},
    {"id": 7, "command": "option7", "desc": "List Master Trims"},
    {"id": 7, "command": "option7", "desc": "Danh sách trims cần thiết"},
    {"id": 7, "command": "option7", "desc": "Các trims đang có trong hệ thống"},

    # option8 - DM Actual
    {"id": 8, "command": "option8", "desc": "Báo cáo thực tế Demand Actual"},
    {"id": 8, "command": "option8", "desc": "Báo cáo DM Actual"},
    {"id": 8, "command": "option8", "desc": "DM Actual"},
    {"id": 8, "command": "option8", "desc": "Tôi muốn xem DM Actual"},
    {"id": 8, "command": "option8", "desc": "Demand thực tế của SC5678"},

    # option9 - Fabric Trans
    {"id": 9, "command": "option9", "desc": "Báo cáo Fabric Trans"},
    {"id": 9, "command": "option9", "desc": "Report Fabric Trans"},
    {"id": 9, "command": "option9", "desc": "Fabric Trans Summary"},
    {"id": 9, "command": "option9", "desc": "Di chuyển vải (fabric)"},

    # option10 - Process WIP
    {"id": 10, "command": "option10", "desc": "Báo cáo Process Wip"},
    {"id": 10, "command": "option10", "desc": "Report Process Wip"},
    {"id": 10, "command": "option10", "desc": "Tiến độ xử lý của các mã GO"},
    {"id": 10, "command": "option10", "desc": "Process WIP theo từng GO"},

    # option11 - Submat Trans
    {"id": 11, "command": "option11", "desc": "Báo cáo Submat Trans"},
    {"id": 11, "command": "option11", "desc": "Report Submat Trans"},
    {"id": 11, "command": "option11", "desc": "Submat Trans Summary"},
    {"id": 11, "command": "option11", "desc": "Phụ liệu đã chuyển sang tổ sản xuất"},

    # option12 - Compare Technical vs Actual
    {"id": 12, "command": "option12", "desc": "So sánh Demand thực tế và kỹ thuật"},
    {"id": 12, "command": "option12", "desc": "So sánh DM Actual và DM Technical"},
    {"id": 12, "command": "option12", "desc": "So sánh giữa SC1234 và SC5678"},
    {"id": 12, "command": "option12", "desc": "Có chênh lệch giữa kỹ thuật và thực tế không?"},
]


def index_sample_data():
    existing = qdrant.scroll(collection_name=collection_name, limit=1000)
    existing_ids = {p.payload.get("id") for p in existing[0]}  # Set of existing IDs

    points = []
    for item in sample_data:
        if item["id"] in existing_ids:
            continue  # Tránh trùng ID
        emb = embedding_model.encode(item["desc"]).tolist()
        points.append(PointStruct(id=item["id"], vector=emb, payload=item))
    if points:
        qdrant.upsert(collection_name=collection_name, points=points)


class ChatMessage:
    def __init__(self, user_id: str, text: str, is_user: bool = True, timestamp: datetime = None):
        self.user_id = user_id
        self.text = text
        self.is_user = is_user
        self.timestamp = timestamp or datetime.now()
        self.id = str(uuid.uuid4())

class ReportPage:
    def __init__(self, page):
        self.selected_rows = set()
        self.page = page
        self.last_data = None

        self.file_picker = ft.FilePicker(on_result=self.save_excel)
        self.page.overlay.append(self.file_picker)
        self._excel_bytes = None  # Biến tạm lưu dữ liệu file

        self.module_func_map = {
            "option1": [  # DM Technical
                ("option1", "Report Technical"),
                ("option2", "List GO"),
                ("option3", "Cutting Forecast"),
                ("option4", "Go Quantity"),
                ("option5", "Submat Demand"),
                ("option6", "Master Fabric List"),
                ("option7", "Master Trims List"),
            ],
            "option2": [  # DM Actual
                ("option8", "Report Actual"),
                ("option9", "Fabric Trans Summary"),
                ("option10", "JO Process Wip"),
                ("option11", "Submat Trans Summary"),
            ],
            "option3": [  # Compare DM
                ("option12", "Report Compare"),
            ]
        }

        init_collection()
        index_sample_data()

        self.messages: List[ChatMessage] = []

        # UI Components
        self.chat_container = ft.ListView(
            expand=True,
            spacing=10,
            padding=ft.padding.all(10),
            auto_scroll=True
        )
        
        self.input_field = ft.TextField(
            hint_text="Nhập tin nhắn của bạn...",
            expand=True,
            multiline=True,
            min_lines=1,
            max_lines=3,
            on_submit=self.send_message,
            border_radius=20,
            filled=True,
            content_padding=ft.padding.symmetric(horizontal=15, vertical=10)
        )
        
        self.send_button = ft.IconButton(
            icon=ft.Icons.SEND,
            on_click=self.send_message,
            bgcolor=ft.Colors.BLUE,
            icon_color=ft.Colors.WHITE,
            tooltip="Gửi tin nhắn"
        )

    def chat_bot(self):
        # Header với settings
        header = ft.Container(
            content=ft.Row(
                controls=[
                    ft.Text(
                        "🤖 AI Chat Assistant", 
                        size=20, 
                        weight=ft.FontWeight.BOLD,
                        color=ft.Colors.BLUE_800
                    ),
                    ft.IconButton(
                        icon=ft.Icons.DELETE_OUTLINE,
                        on_click=self.clear_chat,
                        tooltip="Xóa lịch sử chat"
                    )
                ]
            ),
            padding=ft.padding.all(15),
            bgcolor=ft.Colors.BLUE_50,
            border_radius=ft.border_radius.only(top_left=10, top_right=10)
        )
        
        # Chat area
        chat_area = ft.Container(
            content=self.chat_container,
            expand=True,
            bgcolor=ft.Colors.WHITE,
            padding=ft.padding.all(5)
        )
        
        # Input area
        input_area = ft.Container(
            content=ft.Row(
                controls=[
                    self.input_field,
                    self.send_button
                ],
                spacing=10
            ),
            padding=ft.padding.all(15),
            bgcolor=ft.Colors.GREY_50,
            border_radius=ft.border_radius.only(bottom_left=10, bottom_right=10)
        )
        
        # Main layout
        main_container = ft.Container(
            content=ft.Column(
                controls=[
                    header,
                    chat_area,
                    input_area
                ],
                spacing=0,
                expand=True
            ),
            border_radius=10,
            border=ft.border.all(1, ft.Colors.GREY_300),
            expand=True,
            margin=ft.margin.all(10)
        )
        self.add_welcome_message()
        return main_container

    def create_main_content(self):

        self.comment_text = ft.Text("")

        main_container = self.chat_bot()  # Thêm chatbot trong này

        return ft.Column([
            self.comment_text,

            main_container
        ])

    def query_data(self,selected_data, sub_selected_data, list_go_checked=False):
        sc_nos_str = ''
        if list_go_checked:
            sc_nos_str = self.get_list_go_str()            

        if selected_data == "DM Technical":
            if sub_selected_data == "Report Technical":
                data = SupabaseFunctions().get_data("dm_technical", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                    else SupabaseFunctions().get_data("dm_technical", "*")

            elif sub_selected_data == "List GO":
                data = SupabaseFunctions().get_data("list_go", "*")

            elif sub_selected_data == "Cutting Forecast":
                data = SupabaseFunctions().get_data("cutting_forecast", "*", f' "GO" IN ({sc_nos_str})') if sc_nos_str \
                    else SupabaseFunctions().get_data("cutting_forecast", "*")

            elif sub_selected_data == "Go Quantity":
                data = SupabaseFunctions().get_data("go_quantity", "*", f' "GO_No" IN ({sc_nos_str})') if sc_nos_str \
                    else SupabaseFunctions().get_data("go_quantity", "*")

            elif sub_selected_data == "Submat Demand":
                data = SupabaseFunctions().get_data("submat_demand", "*", f' "GO" IN ({sc_nos_str})') if sc_nos_str \
                    else SupabaseFunctions().get_data("submat_demand", "*")

            elif sub_selected_data == "Master Fabric List":
                data = SupabaseFunctions().get_data("fabric_list", "*")

            elif sub_selected_data == "Master Trims List":
                data = SupabaseFunctions().get_data("trims_list", "*")
        
        elif selected_data == "DM Actual":
            if sub_selected_data == "Report Actual":
                data = SupabaseFunctions().get_data("dm_actual", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                    else SupabaseFunctions().get_data("dm_actual", "*")

            elif sub_selected_data == "Fabric Trans Summary":
                data = SupabaseFunctions().get_data("fabric_trans", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                    else SupabaseFunctions().get_data("fabric_trans", "*")

            elif sub_selected_data == "JO Process Wip":
                data = SupabaseFunctions().get_data("process_wip", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                    else SupabaseFunctions().get_data("process_wip", "*")

            elif sub_selected_data == "Submat Trans Summary":
                data = SupabaseFunctions().get_data("submat_trans", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                    else SupabaseFunctions().get_data("submat_trans", "*")
                
        elif selected_data == "Compare DM":
            if sub_selected_data == "Report Compare":
                data_techncial = SupabaseFunctions().get_data("dm_technical", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                    else SupabaseFunctions().get_data("dm_technical", "*")
                
                data_actual = SupabaseFunctions().get_data("dm_actual", "*", f' "SC_NO" IN ({sc_nos_str})') if sc_nos_str \
                    else SupabaseFunctions().get_data("dm_actual", "*")
                
                data = self.process_data_compare(data_techncial, data_actual)
        
        if data.empty:
            return pd.DataFrame()
        data = data.drop(columns=["id"])
        data = data.sort_values(by=[data.columns[0]])

        return data

    def process_data_compare(self,data_technical, data_actual):
        data = data_technical.merge(data_actual, how='left', on=['SC_NO', 'CODE_CUSTOMS']).rename(columns = {"id_x": "id"})
        cols = ["id", "SC_NO", "CODE_CUSTOMS", "TOTAL_AT", "TOTAL_PCS_AT", "DEMAND_AT", "TOTAL", "TOTAL_PCS", "DEMAND"]
        data = data[cols]

        data["COMPARE"] = ((data["DEMAND_AT"] / data["DEMAND"])*100).where(data["DEMAND"] > 0, 0)
        data["COMPARE"] = data["COMPARE"].apply(lambda x: f"{x:.1f}%" if pd.notnull(x) else "")

        return data

    def list_go_check(self, e):
        self.list_go_checked = e.control.value

    def get_list_go_str(self):        
        data_go = SupabaseFunctions().get_data("list_go", "*")
        sc_nos = data_go["SC_NO"].unique().tolist()
        sc_nos_str = ','.join(f"'{sc_no}'" for sc_no in sc_nos)
        return sc_nos_str

    def on_download_click(self, e):
        file_name = "ket_qua.xlsx"
        # print(self.last_data, self.func_name.split("'")[1])
        # Lưu dữ liệu excel vào biến tạm
        if hasattr(self, "last_data") and self.last_data is not None:
            excel_bytes = self.to_excel(self.last_data)
            self._excel_bytes = excel_bytes
            self.file_picker.save_file(file_name=f"{file_name}.xlsx")
        else:
            self.comment_text.value = "❌ Chưa có dữ liệu để tải!"
            self.page.update()

    def to_excel(self, data):
        output = io.BytesIO()
        print(data)
        # Nếu là dict (nhiều sheet)
        if isinstance(data, dict):
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                for sheet_name, df in data.items():
                    # Đổi tên sheet cho đẹp
                    sheet = {
                        "dm_technical": "DM_Technical",
                        "dm_actual": "DM_Actual",
                        "compare_dm": "Compare_DM"
                    }.get(sheet_name, sheet_name)
                    if df is not None and not df.empty:
                        df.to_excel(writer, index=False, sheet_name=sheet)
        # Nếu là DataFrame (1 sheet)
        elif isinstance(data, pd.DataFrame):
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                data.to_excel(writer, index=False, sheet_name="Sheet1")
        return output.getvalue()

    def save_excel(self, result):
        # result là đối tượng FilePickerResult
        if result.path and self._excel_bytes:
            with open(result.path, "wb") as f:
                f.write(self._excel_bytes)
            self.comment_text.value = "✅ Tài liệu tải xong!"
            self.page.update()

    def add_welcome_message(self):
        """Thêm tin nhắn chào mừng"""
        welcome_text = """👋 Xin chào! Tôi là AI Assistant của bạn!"""
        
        welcome_msg = ChatMessage("assistant", welcome_text, is_user=False)
        self.messages.append(welcome_msg)
        self.display_message(welcome_msg)
        
    def display_message(self, message: ChatMessage):
        """Hiển thị tin nhắn trong chat"""
        
        # Avatar và thông tin người gửi
        if message.is_user:
            avatar = ft.CircleAvatar(
                content=ft.Text("U", color=ft.Colors.WHITE),
                bgcolor=ft.Colors.BLUE,
                radius=15
            )
            align = ft.MainAxisAlignment.END
            bg_color = ft.Colors.BLUE_100
            text_color = ft.Colors.BLACK
            margin_left = 50
            margin_right = 10
        else:
            avatar = ft.CircleAvatar(
                content=ft.Text("🤖", size=16),
                bgcolor=ft.Colors.GREEN,
                radius=15
            )
            align = ft.MainAxisAlignment.START
            bg_color = ft.Colors.GREEN_100
            text_color = ft.Colors.BLACK
            margin_left = 10
            margin_right = 50
            
        # Thời gian
        time_str = message.timestamp.strftime("%H:%M")
        
        # Container tin nhắn
        message_container = ft.Container(
            content=ft.Column(
                controls=[
                    ft.Row(
                        controls=[
                            avatar,
                            ft.Column(
                                controls=[
                                    ft.Container(
                                        content=ft.Text(
                                            message.text,
                                            size=14,
                                            color=text_color,
                                            selectable=True
                                        ),
                                        padding=ft.padding.all(12),
                                        bgcolor=bg_color,
                                        border_radius=15,
                                        margin=ft.margin.only(left=10)
                                    ),
                                    ft.Text(
                                        time_str,
                                        size=10,
                                        color=ft.Colors.GREY_600,
                                        italic=True
                                    )
                                ],
                                spacing=2,
                                expand=True
                            )
                        ],
                        alignment=ft.CrossAxisAlignment.START if not message.is_user else ft.CrossAxisAlignment.END
                    )
                ],
                spacing=2
            ),
            margin=ft.margin.only(left=margin_left, right=margin_right, bottom=5)
        )
        
        self.chat_container.controls.append(message_container)
        self.page.update()
        
    def show_typing_indicator(self):
        """Hiển thị indicator đang gõ"""
        typing_container = ft.Container(
            content=ft.Row(
                controls=[
                    ft.CircleAvatar(
                        content=ft.Text("🤖", size=16),
                        bgcolor=ft.Colors.GREEN,
                        radius=15
                    ),
                    ft.Container(
                        content=ft.Row(
                            controls=[
                                ft.ProgressRing(width=16, height=16, stroke_width=2),
                                ft.Text("AI đang suy nghĩ...", size=12, italic=True)
                            ],
                            spacing=8
                        ),
                        padding=ft.padding.all(12),
                        bgcolor=ft.Colors.GREY_100,
                        border_radius=15,
                        margin=ft.margin.only(left=10)
                    )
                ],
                alignment=ft.CrossAxisAlignment.CENTER
            ),
            margin=ft.margin.only(left=10, right=50, bottom=5)
        )
        
        self.chat_container.controls.append(typing_container)
        self.page.update()
        return typing_container
        
    def remove_typing_indicator(self, typing_container):
        """Xóa typing indicator"""
        if typing_container in self.chat_container.controls:
            self.chat_container.controls.remove(typing_container)
            self.page.update()
    
    async def send_message(self, e=None):
        """Gửi tin nhắn"""
        message_text = self.input_field.value.strip()
        if not message_text:
            return
            
        # Clear input
        self.input_field.value = ""
        self.page.update()
        
        # Add user message
        user_message = ChatMessage("user", message_text, is_user=True)
        self.messages.append(user_message)
        self.display_message(user_message)
        
        # Show typing indicator
        typing_indicator = self.show_typing_indicator()
        
        # Get AI response
        try:
            ai_response = await self.get_ai_response(message_text)
            self.remove_typing_indicator(typing_indicator)
            if isinstance(ai_response, list):
                # Trả về nhiều bảng
                for text, table in ai_response:
                    ai_message = ChatMessage("assistant", text, is_user=False)
                    self.messages.append(ai_message)
                    self.display_message(ai_message)
                    self.chat_container.controls.append(table)
                self.add_download_prompt(text)
                self.page.update()
            elif isinstance(ai_response, tuple):
                # Nếu trả về (text, table)
                text, table = ai_response
                ai_message = ChatMessage("assistant", text, is_user=False)
                self.messages.append(ai_message)
                self.display_message(ai_message)
                # Hiển thị bảng ngay sau tin nhắn
                self.chat_container.controls.append(table)
                self.add_download_prompt(text)
                self.page.update()
            else:
                ai_message = ChatMessage("assistant", ai_response, is_user=False)
                self.messages.append(ai_message)
                self.display_message(ai_message)
        except Exception as ex:
            self.remove_typing_indicator(typing_indicator)
            error_message = ChatMessage("assistant", f"❌ Lỗi: {str(ex)}", is_user=False)
            self.messages.append(error_message)
            self.display_message(error_message)

    def clear_chat(self, e=None):
        """Xóa lịch sử chat"""
        def close_dialog(e):
            if hasattr(self.page, 'dialog') and self.page.dialog:
                self.page.dialog.open = False
                self.page.update()
                
        def confirm_clear(e):
            self.messages.clear()
            self.chat_container.controls.clear()
            self.add_welcome_message()
            close_dialog(e)
            
            # Thông báo
            self.page.snack_bar = ft.SnackBar(
                content=ft.Text("🗑️ Đã xóa lịch sử chat!"),
                bgcolor=ft.Colors.ORANGE
            )
            self.page.snack_bar.open = True
            self.page.update()
            
        confirm_dialog = ft.AlertDialog(
            modal=True,
            title=ft.Text("🗑️ Xóa lịch sử chat", weight=ft.FontWeight.BOLD),
            content=ft.Text("Bạn có chắc chắn muốn xóa toàn bộ lịch sử chat không?"),
            actions=[
                ft.Row(
                    controls=[
                        ft.TextButton(
                            "Hủy", 
                            on_click=close_dialog,
                            style=ft.ButtonStyle(color=ft.Colors.GREY_700)
                        ),
                        ft.ElevatedButton(
                            "Xóa", 
                            on_click=confirm_clear, 
                            bgcolor=ft.Colors.RED, 
                            color=ft.Colors.WHITE
                        )
                    ],
                    alignment=ft.MainAxisAlignment.END,
                    spacing=10
                )
            ],
            actions_alignment=ft.MainAxisAlignment.END
        )
        
        self.page.dialog = confirm_dialog
        confirm_dialog.open = True
        self.page.update()
    
    def search_query(self,query: str, top_k: int):
        try:
            emb = embedding_model.encode(query).tolist()
            hits = qdrant.search(
                collection_name=collection_name,
                query_vector=emb,
                limit=top_k
            )
            if hits:
                return [hit.payload for hit in hits]
        except Exception as e:
            print(f"Lỗi truy vấn: {e}")
        return []
    
    def preprocess_question(self, question):
    # Chuẩn hóa: về chữ thường, bỏ dấu, loại bỏ ký tự đặc biệt nếu cần
        question = question.lower()
        question = ''.join(
            c for c in unicodedata.normalize('NFD', question)
            if unicodedata.category(c) != 'Mn'
        )
        return question

    async def get_ai_response(self, message_text):
        """Lấy trả về AI"""
        # 1. Nhận diện các mã GO/SC_NO trong câu hỏi
        question = self.preprocess_question(message_text)
        go_pattern = r"s\d{2,}[a-z0-9]+"
        go_list = re.findall(go_pattern, question)
        # === ƯU TIÊN RULE-BASED ===
        for keyword, (command, func_name) in RULE_KEYWORDS.items():
            if keyword in question:
                data = self.query_data(
                    selected_data=self.get_module_name_by_key(command),
                    sub_selected_data=func_name
                )
                if data is not None and not data.empty:
                    table = ft.DataTable(
                        columns=[ft.DataColumn(ft.Text(col)) for col in data.columns],
                        rows=[
                            ft.DataRow(
                                cells=[ft.DataCell(ft.Text(str(cell))) for cell in row]
                            ) for row in data.head(5).values.tolist()
                        ]
                    )
                    self.last_data = data
                    return (f"Kết quả cho '{func_name}':", table)
                else:
                    return f"Không có dữ liệu cho '{func_name}'."
            
        if go_list:
            sc_nos_str = ",".join(f"'{go}'" for go in go_list)
            # Truy vấn từng bảng
            self.last_data = {}
            results = []
            # DM Technical
            data_tech = SupabaseFunctions().get_data("dm_technical", "*", f' "SC_NO" IN ({sc_nos_str})')
            if data_tech is not None and not data_tech.empty:
                table_tech = ft.DataTable(
                    columns=[ft.DataColumn(ft.Text(col)) for col in data_tech.columns],
                    rows=[
                        ft.DataRow(
                            cells=[ft.DataCell(ft.Text(str(cell))) for cell in row]
                        ) for row in data_tech.head(5).values.tolist()
                    ]
                )
                results.append(("DM Technical:", table_tech))
                self.last_data["dm_technical"] = data_tech
            # DM Actual
            data_actual = SupabaseFunctions().get_data("dm_actual", "*", f' "SC_NO" IN ({sc_nos_str})')
            if data_actual is not None and not data_actual.empty:
                table_actual = ft.DataTable(
                    columns=[ft.DataColumn(ft.Text(col)) for col in data_actual.columns],
                    rows=[
                        ft.DataRow(
                            cells=[ft.DataCell(ft.Text(str(cell))) for cell in row]
                        ) for row in data_actual.head(5).values.tolist()
                    ]
                )
                results.append(("DM Actual:", table_actual))
                self.last_data["dm_actual"] = data_actual
            # Compare DM
            data_compare = self.process_data_compare(data_tech, data_actual)
            if data_compare is not None and not data_compare.empty:
                table_compare = ft.DataTable(
                    columns=[ft.DataColumn(ft.Text(col)) for col in data_compare.columns],
                    rows=[
                        ft.DataRow(
                            cells=[ft.DataCell(ft.Text(str(cell))) for cell in row]
                        ) for row in data_compare.head(5).values.tolist()
                    ]
                )
                results.append(("Compare DM:", table_compare))
                self.last_data["compare_dm"] = data_compare
            if results:
                # Trả về list các tuple (text, table)
                return results
            else:
                return "Không tìm thấy dữ liệu cho các mã GO/SC_NO bạn yêu cầu."
        else:
            # 1. Tìm lệnh gần đúng nhất
            results = self.search_query(message_text, top_k=3)  # Tăng top_k lên 3
            if results:
                for result in results:
                    func_label = result.get("desc", "").lower()
                    func_command = result.get("command", "")
                    if self.preprocess_question(func_label) in question:
                        # Mapping command sang tên chức năng trong module_func_map
                        for module_key, func_list in self.module_func_map.items():
                            for func_code, func_name in func_list:
                                if func_command == func_code or func_label.lower() in func_name.lower():
                                    # Truy vấn dữ liệu
                                    data = self.query_data(
                                        selected_data=self.get_module_name_by_key(module_key),
                                        sub_selected_data=func_name
                                    )
                                    if data is not None and not data.empty:
                                        # Tạo bảng Flet DataTable từ pandas DataFrame
                                        table = ft.DataTable(
                                            columns=[ft.DataColumn(ft.Text(col)) for col in data.columns],
                                            rows=[
                                                ft.DataRow(
                                                    cells=[ft.DataCell(ft.Text(str(cell))) for cell in row]
                                                ) for row in data.head(5).values.tolist()
                                            ]
                                        )
                                        self.last_data = data
                                        # Trả về tuple (text, table) để xử lý hiển thị ở display_message
                                        return (f"Kết quả cho '{func_name}':", table)
                                    else:
                                        return f"Không tìm thấy dữ liệu cho '{func_name}'."
                # Nếu không có match nào chắc chắn, gợi ý các lệnh gần đúng
                suggestions = [f"👉 {r['desc']}" for r in results]
                return "❓ Có thể bạn đang muốn hỏi:\n" + "\n".join(suggestions)
            else:
                # Không có GO, tìm lệnh gần đúng nhất
                results = self.search_query(message_text, top_k=3)
                if results:
                    suggestions = [f"👉 {item['desc']}" for item in results]
                    return "Tôi chưa hiểu rõ câu hỏi, bạn có muốn hỏi:\n" + "\n".join(suggestions)
                else:
                    return "❌ Không tìm thấy chức năng phù hợp với câu hỏi của bạn. Vui lòng hỏi rõ hơn."


    def get_module_name_by_key(self, key):
        # Trả về tên module theo key (ví dụ: "option1" -> "DM Technical")
        mapping = {
            "option1": "DM Technical",
            "option2": "DM Actual",
            "option3": "Compare DM"
        }
        return mapping.get(key, key)

    def add_download_prompt(self, func_name,e=None):
        """Thêm prompt hỏi tải về và nút tải về"""
        self.func_name = func_name
        prompt = ft.Text("Bạn có muốn tải dữ liệu về không?")
        download_btn = ft.ElevatedButton(
            "Tải về",
            icon=ft.Icons.DOWNLOAD,
            bgcolor=ft.Colors.BLUE_400,
            color=ft.Colors.WHITE,
            on_click=self.on_download_click  # Đã có sẵn hàm này
        )
        self.chat_container.controls.append(
            ft.Row([prompt, download_btn], alignment=ft.MainAxisAlignment.END, spacing=10)
        )
        self.page.update()