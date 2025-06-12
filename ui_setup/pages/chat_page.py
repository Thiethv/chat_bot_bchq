import io
import os
from typing import List, Dict, Any
import flet as ft
import pandas as pd
from datetime import datetime
import uuid
import unicodedata

from ui_setup.utils.task_manager import AsyncQueryEngine

# Constants
COLLECTION_NAME = "command_embeddings"

class ChatMessage:
    def __init__(self, user_id: str, text: str, is_user: bool = True, timestamp: datetime = None):
        self.user_id = user_id
        self.text = text
        self.is_user = is_user
        self.timestamp = timestamp or datetime.now()
        self.id = str(uuid.uuid4())

class ChatPage:
    def __init__(self, page: ft.Page):
        self.page = page
        self.selected_rows = set()
        self.last_data = None
        self.messages: List[ChatMessage] = []
        self.query_engine = AsyncQueryEngine()
        
        self._embedding_initialized = False
        
        # File picker setup
        self.file_picker = ft.FilePicker()
        self.page.overlay.append(self.file_picker)
        self._excel_bytes = None

        self.data_history = []

        self.uploaded_file_data = None        
        
        # UI Components
        self._init_ui_components()
    
    def _init_ui_components(self):
        """Khởi tạo UI components"""
        self.chat_container = ft.ListView(
            expand=True,
            spacing=10,
            padding=ft.padding.all(10),
            auto_scroll=True
        )
        
        self.input_field = ft.TextField(
            hint_text="Nhập câu hỏi của bạn... (VD: 'Xem DM Technical cho S24M123456') (shift + Enter xuống dòng)",
            expand=True,
            multiline=True,
            min_lines=1,
            max_lines=3,
            border_radius=20,
            filled=True,
            shift_enter=True,
            content_padding=ft.padding.symmetric(horizontal=15, vertical=10),
            on_submit=self.send_message
        )
        
        self.send_button = ft.IconButton(
            icon=ft.Icons.SEND,
            on_click=self.send_message,
            bgcolor=ft.Colors.BLUE,
            icon_color=ft.Colors.WHITE,
            tooltip="Gửi tin nhắn"
        )

        self.add_button = ft.IconButton(
            icon=ft.Icons.ADD,
            on_click=self.add_file,
            bgcolor=ft.Colors.BLUE,
            icon_color=ft.Colors.WHITE,
            tooltip="Tải file"
        )
        
        self.comment_text = ft.Text("")
        
    def chat_bot(self):
        """Tạo giao diện chatbot"""
        header = ft.Container(
            content=ft.Row(
                controls=[
                    ft.Text(
                        "🤖 AI Assistant - Hỏi về dữ liệu", 
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
        
        chat_area = ft.Container(
            content=self.chat_container,
            expand=True,
            bgcolor=ft.Colors.WHITE,
            padding=ft.padding.all(5)
        )
        
        input_area = ft.Container(
            content=ft.Row(
                controls=[
                    self.input_field,
                    self.add_button
                ],
                spacing=10
            ),
            padding=ft.padding.all(15),
            bgcolor=ft.Colors.GREY_50,
            border_radius=ft.border_radius.only(bottom_left=10, bottom_right=10)
        )
        
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
        """Tạo nội dung chính"""
        return ft.Column([
            self.comment_text,
            self.chat_bot()
        ])
    
    async def get_ai_response(self, message_text: str) -> Any:
        try:
            context = self._build_context(message_text)
            result = await self.query_engine.process_query_with_tasks(message_text, context)
            return self._format_ai_result(result, original_query=message_text)
        
        except ConnectionError:
            return "❌ Lỗi kết nối cơ sở dữ liệu. Vui lòng thử lại."
        except TimeoutError:
            return "❌ Hết thời gian chờ. Vui lòng thử lại."
        except Exception as e:
            print(f"Unexpected error in get_ai_response: {e}")
            return "❌ Có lỗi không mong muốn xảy ra. Vui lòng thử lại."
        
    def _build_context(self, message_text: str) -> dict:
        return {
            'file_data': self.last_data,
            'query': message_text,
            'add_process_message': self.add_progress_message
        }
    
    def _format_ai_result(self, result: dict, original_query: str) -> Any:
        result_type = result.get("type")

        if result_type == "no_task":
            self._log_unmatched_query(original_query)
            suggestions = "\n".join([f"• {s}" for s in result.get('suggestions', [])])
            return f"❓ {result['message']}\n\n💡 Bạn muốn báo cáo về loại dữ liệu nào?:\n{suggestions}"

        elif result_type == "missing_conditions":
            msg = f"📋 {result['message']}\n" + "\n".join(result['missing_conditions'])
            if result.get("example"):
                msg += f"\n\n{result['example']}"
            return msg

        elif result_type == "success":
            return self._handle_success_data(result)

        elif result_type == "error":
            return f"❌ {result['message']}"

        return "⚠️ Phản hồi không xác định."

    def _log_unmatched_query(self, query, task=None):
        os.makedirs("logs", exist_ok=True)
        with open("logs/unmatched_queries.txt", "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} >>> {query}")
            if task:
                f.write(f" => {task}")
            f.write("\n")

    def _handle_success_data(self, result):
        data = result["data"]
        if isinstance(data, dict):
            return {"type": "table_choices", "tables": data}
        elif isinstance(data, pd.DataFrame) and not data.empty:
            return {
                "type": "single_table",
                "table": data,
                "table_name": result.get("task_name", "Kết quả"),
                "text": f"📊 {result['task_description']} ({len(data)} bản ghi):"
            }
        elif isinstance(data, dict) and data.get("type") == "error":
            return data["message"]
        else:
            return "❌ Không có dữ liệu nào được tìm thấy cho yêu cầu của bạn."
    
    def show_table_in_chat(self, query_idx, table_name):
        if 0 <= query_idx < len(self.data_history):
            df = self.data_history[query_idx]["tables"].get(table_name)
            if df is not None and not df.empty:
                table = self._create_data_table(df.head(10))
                table_container = ft.Container(
                    content=ft.Column([
                        ft.Row(
                            [
                                ft.Text(f"📊 {table_name.replace('_',' ').title()}", size=15, weight=ft.FontWeight.BOLD),
                                ft.IconButton(
                                    icon=ft.Icons.CLOSE,
                                    tooltip="Đóng bảng này",
                                    on_click=None  # Gán sau
                                )
                            ],
                            alignment=ft.MainAxisAlignment.SPACE_BETWEEN
                        ),
                        table
                    ]),
                    bgcolor=ft.Colors.GREY_50,
                    border_radius=10,
                    padding=10
                )
                close_btn = table_container.content.controls[0].controls[1]
                close_btn.on_click = lambda e, ctl=table_container: self.close_table_in_chat(ctl)
                self.chat_container.controls.append(table_container)
                self.page.update()

    def close_table_in_chat(self, table_container):
        if table_container in self.chat_container.controls:
            self.chat_container.controls.remove(table_container)
            self.page.update()            
    
    def download_one_table(self,query_idx, table_name):
        if 0 <= query_idx < len(self.data_history):
            df = self.data_history[query_idx]["tables"].get(table_name)
            if df is not None and not df.empty:
                excel_bytes = self.to_excel({table_name: df})
                self._excel_bytes = excel_bytes
                self.file_picker.on_result = self.save_excel
                self.file_picker.save_file(file_type=ft.FilePickerFileType.CUSTOM, allowed_extensions=["xlsx"])
    
    def _create_data_table(self, data: pd.DataFrame) -> ft.DataTable:
        """Tạo DataTable từ DataFrame"""
        
        if "id" in data.columns:
            data = data.drop(columns=["id"])

        return ft.DataTable(
            columns=[ft.DataColumn(ft.Text(col, weight=ft.FontWeight.BOLD)) for col in data.columns],
            rows=[
                ft.DataRow(
                    cells=[ft.DataCell(ft.Text(str(cell))) for cell in row]
                ) for row in data.values.tolist()
            ],
            border=ft.border.all(1, ft.Colors.GREY_300),
            border_radius=5,
            vertical_lines=ft.BorderSide(1, ft.Colors.GREY_200),
            horizontal_lines=ft.BorderSide(1, ft.Colors.GREY_200),
        )
        
    def preprocess_question(self, question: str) -> str:
        """Preprocess question"""
        question = question.lower().strip()
        # Remove Vietnamese accents
        question = ''.join(
            c for c in unicodedata.normalize('NFD', question)
            if unicodedata.category(c) != 'Mn'
        )
        return question
    
    def list_available_tasks(self) -> str:
        """Trả về danh sách các tác vụ có sẵn"""
        tasks = self.query_engine.task_manager.tasks
        task_list = []
        
        for task_name, task_def in tasks.items():
            conditions_text = []
            for cond in task_def.conditions:
                status = "bắt buộc" if cond.required else "tùy chọn"
                conditions_text.append(f"  - {cond.description} ({status})")
            
            task_info = f"📋 **{task_def.description}**\n" + "\n".join(conditions_text)
            task_list.append(task_info)
        
        return "🔧 **CÁC TÁC VỤ CÓ SẴN:**\n\n" + "\n\n".join(task_list)

    # UI Methods
    def add_welcome_message(self):
        """Add welcome message"""
        welcome_text = """👋 Xin chào! Tôi là AI Assistant.

💡 Bạn có thể hỏi tôi:
• "Tính định mức kỹ thuật cho GO S24M12345 hoặc JO 24M12345JP01"
• "Báo cáo so sánh định mức cho GO/JO"  
• "Cutting forecast cho GO/JO"

Hãy thử hỏi tôi về dữ liệu bạn cần! 🚀"""
        
        welcome_msg = ChatMessage("assistant", welcome_text, is_user=False)
        self.messages.append(welcome_msg)
        self.display_message(welcome_msg)
    
    def display_message(self, message: ChatMessage):
        """Display message in chat"""
        if message.is_user:
            avatar = ft.CircleAvatar(
                content=ft.Text("U", color=ft.Colors.WHITE),
                bgcolor=ft.Colors.BLUE,
                radius=15
            )
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
            bg_color = ft.Colors.GREEN_100
            text_color = ft.Colors.BLACK
            margin_left = 10
            margin_right = 50
        
        time_str = message.timestamp.strftime("%H:%M")
        
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
    
    def add_progress_message(self, text):
        msg = ChatMessage("assistant", text, is_user=False)
        self.messages.append(msg)
        self.display_message(msg)
        self.page.update()
        return msg

    def show_typing_indicator(self):
        """Show typing indicator"""
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
                                ft.Text("AI đang phân tích và lấy dữ liệu...", size=12, italic=True)
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
        """Remove typing indicator"""
        if typing_container in self.chat_container.controls:
            self.chat_container.controls.remove(typing_container)
            self.page.update()
    
    async def send_message(self, e=None):
        message_text = self.input_field.value.strip()
        if not message_text:
            return

        user_message = ChatMessage("user", message_text, is_user=True)
        self.messages.append(user_message)
        self.display_message(user_message)
        self.input_field.value = ""
        self.page.update()

        if not getattr(self, "_warmed_up", False):
            await self.query_engine.warm_up_connections()
            self._warmed_up = True

        typing_indicator = self.show_typing_indicator()

        try:
            ai_response = await self.get_ai_response(message_text)
            self.remove_typing_indicator(typing_indicator)

            if ai_response is None:
                # Không append None vào chat
                self.add_download_prompt()

            elif isinstance(ai_response, dict) and ai_response.get("type") in ["table_choices", "single_table"]:
                if ai_response["type"] == "table_choices":
                    tables = ai_response["tables"]
                else:
                    table_name = ai_response.get("table_name", "Kết quả")
                    tables = {table_name: ai_response["table"]}

                self.data_history.append({
                    "query": message_text,
                    "timestamp": datetime.now(),
                    "tables": tables
                })

                table_buttons = []
                current_query_idx = len(self.data_history) - 1
                
                for table_name, df in tables.items():
                    btn = ft.ElevatedButton(
                        f"Xem {table_name.replace('_',' ').title()} ({len(df)} dòng)",
                        on_click=lambda e, idx=current_query_idx, tn=table_name: self.show_table_in_chat(idx, tn)
                    )
                    download_btn = ft.IconButton(
                        icon=ft.Icons.DOWNLOAD,
                        tooltip=f"Tải {table_name.replace('_',' ').title()}",
                        on_click=lambda e, idx=current_query_idx, tn=table_name: self.download_one_table(idx, tn)
                    )
                    table_buttons.append(ft.Row([btn, download_btn], spacing=10))
                        # table_buttons.append(btn)
                msg = ChatMessage("assistant", "🗂️ Chọn bảng bạn muốn xem hoặc tải xuống:", is_user=False)
                self.messages.append(msg)
                self.display_message(msg)
                self.chat_container.controls.extend(table_buttons)
                if ai_response["type"] == "table_choices":
                    self.add_download_prompt()
                    self.last_data = tables

            elif isinstance(ai_response, str):
                ai_message = ChatMessage("assistant", ai_response, is_user=False)
                self.messages.append(ai_message)
                self.display_message(ai_message)

            else:
                # fallback
                ai_message = ChatMessage("assistant", str(ai_response), is_user=False)
                self.messages.append(ai_message)
                self.display_message(ai_message)

            self.page.update()
        except Exception as ex:
            self.remove_typing_indicator(typing_indicator)
            error_message = ChatMessage("assistant", f"❌ Lỗi: {str(ex)}", is_user=False)
            self.messages.append(error_message)
            self.display_message(error_message)
    
    def clear_chat(self, e=None):
        """Clear chat history"""
        def close_dialog(e):
            if hasattr(self.page, 'dialog') and self.page.dialog:
                self.page.dialog.open = False
                self.page.update()
        
        def confirm_clear(e):
            self.messages.clear()
            self.chat_container.controls.clear()
            self.add_welcome_message()
            close_dialog(e)
            
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
                        ft.TextButton("Hủy", on_click=close_dialog),
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
            ]
        )
        
        self.page.dialog = confirm_dialog
        confirm_dialog.open = True
        self.page.update()
    
    def add_download_prompt(self):
        """Add download prompt"""
        prompt = ft.Text("Bạn có muốn tải tất cả dữ liệu này về không?")
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
    
    def on_download_click(self, e):
        # print(self.last_data, self.func_name.split("'")[1])
        # Lưu dữ liệu excel vào biến tạm
        if hasattr(self, "last_data") and self.last_data is not None:
            data = self.last_data
            excel_bytes = self.to_excel(data)
            self._excel_bytes = excel_bytes
            self.file_picker.on_result = self.save_excel
            self.file_picker.save_file(file_type=ft.FilePickerFileType.CUSTOM, allowed_extensions=["xlsx"])
        else:
            self.comment_text.value = "❌ Chưa có dữ liệu để tải!"
            self.page.update()

    def to_excel(self, data):
        output = io.BytesIO()
        # Nếu là dict (nhiều sheet)
        if isinstance(data, dict):
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                for sheet_name, df in data.items():
                    
                    # Đổi tên sheet cho đẹp
                    sheet = {
                        "dm_technical": "DM_Technical",
                        "dm_actual": "DM_Actual", 
                        "cutting_forecast": "Cutting_Forecast",
                        "submat_demand": "Submat_Demand",
                        "fabric_trans": "Fabric_Trans",
                        "submat_trans": "Submat_Trans",
                        "process_wip": "Process_WIP",
                        "go_quantity": "GO_Quantity",
                        "compare_dm": "Compare_DM"
                    }.get(sheet_name, sheet_name)
                    if df is not None and not df.empty:
                        df = df.drop(columns=["id"])
                        df.to_excel(writer, index=False, sheet_name=sheet)
        # Nếu là DataFrame (1 sheet)
        elif isinstance(data, pd.DataFrame):
            data = data.drop(columns=["id"])
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                data.to_excel(writer, index=False, sheet_name="Sheet1")
        return output.getvalue()

    def save_excel(self, result):
        # result là đối tượng FilePickerResult
        if result.path and self._excel_bytes:
            file_path = result.path
            if not file_path.lower().endswith(".xlsx"):
                file_path += ".xlsx"

            with open(file_path, "wb") as f:
                f.write(self._excel_bytes)
            self.comment_text.value = "✅ Tài liệu tải xong!"
            self.page.update()

    def add_file(self, e):
        ''' tải file excel lên app và lưu vào dataframe '''
        def on_file_selected(result):
            if result.files and len(result.files) > 0:
                file_path = result.files[0].path
                try:
                    df = pd.read_excel(file_path)
                    self.uploaded_file_data = df
                    self.display_message(ChatMessage("user", f"✅ Đã tải file: {file_path} ({len(df)} dòng)"))
                    
                except Exception as ex:
                    self.comment_text.value = f"❌ Lỗi khi đọc file: {ex}"
                self.page.update()

        self.file_picker.on_result = on_file_selected       
        self.file_picker.pick_files(allow_multiple=False, allowed_extensions=["xlsx", "xls"])

        