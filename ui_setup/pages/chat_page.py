import io
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
            return f"❓ {result['message']}\n\n💡 Bạn có thể thử:\n{suggestions}"

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

    def _log_unmatched_query(self, query):
        with open("logs/unmatched_queries.txt", "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} >>> {query}\n")

    def _handle_success_data(self, result):
        data = result["data"]
                
        # Format dữ liệu trả về
        if isinstance(data, dict):
            if data.get("type") == "error":
                return data["message"]
            # Multiple tables - hiển thị từng cái một
            self.last_data = {}
            
            for table_name, df in data.items():
                if not df.empty:
                    display_data = df.head(5)
                    table = self._create_data_table(display_data)
                    table_display_name = table_name.replace("_", " ").title()
                    # Hiển thị ngay lập tức từng kết quả
                    step_message = ChatMessage(
                        "assistant", 
                        f"📊 {table_display_name} ({len(df)} bản ghi):", 
                        is_user=False
                    )
                    self.messages.append(step_message)
                    self.display_message(step_message)
                    self.chat_container.controls.append(table)
                    self.page.update()

                    self.last_data[table_name] = df

            return None
        
        elif isinstance(data, pd.DataFrame) and not data.empty:
            # Single table
            display_data = data.head(5)
            table = self._create_data_table(display_data)
            self.last_data = data
            return (f"📊 {result['task_description']} ({len(data)} bản ghi):", table)
        
        elif isinstance(data, dict) and data.get("type") == "error":
            return data["message"]
        
        else:
            return "❌ Không có dữ liệu nào được tìm thấy cho yêu cầu của bạn."

    '''async def get_ai_response(self, message_text: str) -> Any:
        """Improved AI response với async processing"""
        try:
            # Lấy context từ UI (file data, etc.)
            context = {}
            if hasattr(self, 'last_data') and self.last_data is not None:
                context['file_data'] = self.last_data

            # Thêm callback để hiển thị progress
            context['add_process_message'] = self.add_progress_message

            context['query'] = message_text
            
            # Process với task-based approach
            result = await self.query_engine.process_query_with_tasks(
                message_text, 
                context
            )
            
            if result["type"] == "no_task":
                return f"❓ {result['message']}\n\n💡 Bạn có thể thử:\n" + "\n".join([f"• {s}" for s in result['suggestions']])
            
            elif result["type"] == "missing_conditions":
                message = f"📋 {result['message']}\n" + "\n".join(result['missing_conditions'])
                if result.get("example"):
                    message += f"\n\n{result['example']}"
                return message
            
            elif result["type"] == "success":
                data = result["data"]
                
                # Format dữ liệu trả về
                if isinstance(data, dict):
                    if data.get("type") == "error":
                        return data["message"]
                    # Multiple tables - hiển thị từng cái một
                    self.last_data = {}
                    
                    for table_name, df in data.items():
                        if not df.empty:
                            display_data = df.head(5)
                            table = self._create_data_table(display_data)
                            table_display_name = table_name.replace("_", " ").title()
                            # Hiển thị ngay lập tức từng kết quả
                            step_message = ChatMessage(
                                "assistant", 
                                f"📊 {table_display_name} ({len(df)} bản ghi):", 
                                is_user=False
                            )
                            self.messages.append(step_message)
                            self.display_message(step_message)
                            self.chat_container.controls.append(table)
                            self.page.update()

                            self.last_data[table_name] = df

                    return None
                
                elif isinstance(data, pd.DataFrame) and not data.empty:
                    # Single table
                    display_data = data.head(5)
                    table = self._create_data_table(display_data)
                    self.last_data = data
                    return (f"📊 {result['task_description']} ({len(data)} bản ghi):", table)
                
                elif isinstance(data, dict) and data.get("type") == "error":
                    return data["message"]
                
                else:
                    return "❌ Không có dữ liệu nào được tìm thấy cho yêu cầu của bạn."
            
            elif result["type"] == "error":
                return f"❌ {result['message']}"
                
        except ConnectionError:
            return "❌ Lỗi kết nối cơ sở dữ liệu. Vui lòng thử lại."
        except TimeoutError:
            return "❌ Hết thời gian chờ. Vui lòng thử lại."
        except Exception as e:
            print(f"Unexpected error in get_ai_response: {e}")
            return "❌ Có lỗi không mong muốn xảy ra. Vui lòng thử lại." '''
    
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
    
    def _format_results(self, results: Dict[str, pd.DataFrame], codes: List[str]) -> Any:
        """Format kết quả trả về"""
        if not results or all(df.empty for df in results.values()):
            codes_text = ", ".join(codes)
            return f"❌ Không tìm thấy dữ liệu cho mã: {codes_text}"
        
        # Prepare multiple results
        formatted_results = []
        self.last_data = {}
        
        for table_name, data in results.items():
            if not data.empty:
                # Limit display data
                display_data = data.head(10)
                table = self._create_data_table(display_data)
                
                # Format table name
                table_display_name = table_name.replace("_", " ").title()
                formatted_results.append((f"📊 {table_display_name} ({len(data)} bản ghi):", table))
                self.last_data[table_name] = data
        
        return formatted_results if len(formatted_results) > 1 else formatted_results[0]
    
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
        """Send message"""
        message_text = self.input_field.value.strip()
        if not message_text:
            return
        
        # Add user message
        user_message = ChatMessage("user", message_text, is_user=True)
        self.messages.append(user_message)
        self.display_message(user_message)
        
        # Clear input
        self.input_field.value = ""
        self.page.update()

        # Warm up nếu cần
        if not getattr(self, "_warmed_up", False):
            await self.query_engine.warm_up_connections()
            self._warmed_up = True
        
        # Show typing indicator
        typing_indicator = self.show_typing_indicator()
        
        try:
            # Get AI response
            ai_response = await self.get_ai_response(message_text)
            self.remove_typing_indicator(typing_indicator)

            if ai_response is None:
                # Đã hiển thị từng bước, chỉ cần thêm download prompt
                self.add_download_prompt()
            
            elif isinstance(ai_response, list):
                # Multiple results
                for text, table in ai_response:
                    ai_message = ChatMessage("assistant", text, is_user=False)
                    self.messages.append(ai_message)
                    self.display_message(ai_message)
                    self.chat_container.controls.append(table)
                self.add_download_prompt()

            elif isinstance(ai_response, tuple):
                # Single result with table
                text, table = ai_response
                ai_message = ChatMessage("assistant", text, is_user=False)
                self.messages.append(ai_message)
                self.display_message(ai_message)
                self.chat_container.controls.append(table)
                self.add_download_prompt()
            else:
                # Text only
                ai_message = ChatMessage("assistant", ai_response, is_user=False)
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

            with open(result.path, "wb") as f:
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
                    self.last_data = df
                    self.display_message(ChatMessage("user", f"✅ Đã tải file: {file_path} ({len(df)} dòng)"))
                    
                except Exception as ex:
                    self.comment_text.value = f"❌ Lỗi khi đọc file: {ex}"
                self.page.update()

        self.file_picker.on_result = on_file_selected       
        self.file_picker.pick_files(allow_multiple=False, allowed_extensions=["xlsx", "xls"])
        