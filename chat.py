import io
from typing import List, Optional, Dict, Any
import flet as ft
import pandas as pd
from database.connect_supabase import SupabaseFunctions
from datetime import datetime
import uuid
import unicodedata
import re
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading

from ui_setup.data_dmkt.data_master_list import MasterList

# Lazy loading cho các thư viện nặng
_embedding_model = None
_qdrant = None
_embedding_model_lock = threading.Lock()
_qdrant_lock = threading.Lock()

def get_embedding_model():
    """Lazy loading cho embedding model"""
    global _embedding_model
    if _embedding_model is None:
        with _embedding_model_lock:
            if _embedding_model is None:
                from sentence_transformers import SentenceTransformer
                _embedding_model = SentenceTransformer("thenlper/gte-base")
    return _embedding_model

def get_qdrant_client():
    """Lazy loading cho Qdrant client"""
    global _qdrant
    if _qdrant is None:
        with _qdrant_lock:
            if _qdrant is None:
                import qdrant_client
                _qdrant = qdrant_client.QdrantClient(path="./qdrant_data")
    return _qdrant

# Constants
COLLECTION_NAME = "command_embeddings"
THREAD_POOL = ThreadPoolExecutor(max_workers=2)

# Improved rule mapping với scoring
RULE_PATTERNS = {
    "compare": {
        "keywords": ["compare", "so sánh", "comparison", "báo cáo", "hải quan", "report"],
        "patterns": [r"so\s*sánh", r"compare", r"báo\s*cáo", r"hải\s*quan", r"report"],
        "score": 10
    },
    "dm_technical": {
        "keywords": ["dm technical", "report technical", "kỹ thuật", "technical"],
        "patterns": [r"dm\s*technical", r"định\s*mức\s*kỹ\s*thuật"],
        "score": 9
    },
    "dm_actual": {
        "keywords": ["dm actual", "report actual", "thực tế", "actual"],
        "patterns": [r"dm\s*actual", r"định\s*mức\s*thực\s*tế"],
        "score": 9
    },
    "process_wip": {
        "keywords": ["process wip", "tiến độ", "wip"],
        "patterns": [r"process\s*wip", r"tiến\s*độ"],
        "score": 8
    },
    "list_go": {
        "keywords": ["list go", "danh sách go", "go"],
        "patterns": [r"list\s*go", r"danh\s*sách\s*go"],
        "score": 8
    },
    "cutting_forecast": {
        "keywords": ["cutting forecast", "dự báo cắt", "forecast"],
        "patterns": [r"cutting\s*forecast", r"dự\s*báo\s*cắt"],
        "score": 8
    },
    "submat_demand": {
        "keywords": ["submat demand", "phụ liệu", "demand"],
        "patterns": [r"submat\s*demand", r"phụ\s*liệu"],
        "score": 8
    },
    
    "fabric_trans": {
        "keywords": ["fabric trans summary", "fabric trans", "fabric"],
        "patterns": [r"fabric\s*trans\s*summary", r"fabric\s*trans", r"fabric"],
        "score": 8
    },
    "submat_trans": {
        "keywords": ["submat trans summary", "submat trans", "submat"],
        "patterns": [r"submat\s*trans\s*summary", r"submat\s*trans", r"submat"],
        "score": 8
    },
    "insert_trims_list": {
        "keywords": ["insert trims list", "update trims list", "cập nhật trims", "tải lên trims"],
        "patterns": [r"insert\s*trims\s*list", r"update\s*trims\s*list", r"cập nhật\s*trims", r"tải\s*lên\s*trims"],
        "score": 9
    },
    "insert_fabric_list": {
        "keywords": ["insert fabric list", "update fabric list", "cập nhật fabric list", "tải lên fabric list"],
        "patterns": [r"insert\s*fabric\s*list", r"update\s*fabric\s*list", r"cập nhật\s*fabric\s*list", r"tải\s*lên\s*fabric\s*list"],
        "score": 9
    },
    "insert_range_dm": {
        "keywords": ["insert range dm", "update range dm", "cập nhật range dm", "tải lên range dm"],
        "patterns": [r"insert\s*range\s*dm", r"update\s*range\s*dm", r"cập nhật\s*range\s*dm", r"tải\s*lên\s*range\s*dm"],
        "score": 9
    }
}

# Function mapping
FUNCTION_MAPPING = {
    "dm_technical": ("DM Technical", "Report Technical"),
    "dm_actual": ("DM Actual", "Report Actual"),
    "process_wip": ("DM Actual", "JO Process Wip"),
    "list_go": ("DM Technical", "List GO"),
    "cutting_forecast": ("DM Technical", "Cutting Forecast"),
    "submat_demand": ("DM Technical", "Submat Demand"),
    "compare": ("Compare DM", "Report Compare"),
    "fabric_trans": ("DM Actual", "Fabric Trans Summary"),
    "submat_trans": ("DM Actual", "Submat Trans Summary"),
    "master_fabric": ("DM Technical", "Master Fabric List"),
    "master_trims": ("DM Technical", "Master Trims List"),
    "go_quantity": ("DM Technical", "Go Quantity")
}

class AsyncQueryEngine:
    """Async query engine cho việc tìm kiếm và xử lý dữ liệu"""
    
    def __init__(self):
        self.supabase = SupabaseFunctions()
    
    async def _warm_up_connections(self):
        """Warm up database connections"""
        try:
            await asyncio.sleep(0.1)  # Đợi app khởi động xong
            # Warm up database connection
            await asyncio.to_thread(self.supabase.get_data, "list_go LIMIT 1", ' "SC_NO" ')
        except Exception as e:
            print(f"Warm up error: {e}")
    
    def score_query_match(self, query: str, normalized_query: str) -> Dict[str, float]:
        """Tính điểm matching cho query"""
        scores = {}
        
        for func_name, config in RULE_PATTERNS.items():
            score = 0

            # Keyword matching
            for keyword in config["keywords"]:
                if keyword in normalized_query:
                    score += config["score"]
            
            # Pattern matching
            for pattern in config["patterns"]:
                if re.search(pattern, normalized_query, re.IGNORECASE):
                    score += config["score"] + 2
            
            # Fuzzy matching bonus
            if any(keyword in query.lower() for keyword in config["keywords"]):
                score += 1
            
            if score > 0:
                scores[func_name] = score
        
        return scores
    
    def extract_codes(self, query: str) -> List[str]:
        """Extract GO/SC codes từ query"""
        patterns = [
            r"s\d{2}m[a-z0-9]+",  # SC codes
            r"\d{2}m\d{5}[a-z]{2}\d{2}",          # JO codes  
            # r"\b[A-Z]{2}\d{4,}",  # Generic codes
        ]
        
        codes = []
        for pattern in patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            codes.extend(matches)
        
        return list(set(codes))  # Remove duplicates
    
    async def get_data_async(self, table: str, columns: str = "*", condition: str = "") -> pd.DataFrame:
        """Async data retrieval"""
        try:
            return await asyncio.to_thread(self.supabase.get_data, table, columns, condition)
        except Exception as e:
            print(f"Data retrieval error for {table}: {e}")
            return pd.DataFrame()
    
    async def process_multiple_queries(self, queries: List[tuple]) -> Dict[str, pd.DataFrame]:
        """Xử lý nhiều queries đồng thời"""
        tasks = []
        for table, columns, condition in queries:
            task = self.get_data_async(table, columns, condition)
            tasks.append((table, task))
        
        results = {}
        for table, task in tasks:
            try:
                data = await task
                if not data.empty and 'id' in data.columns:
                    data = data.drop(columns=["id"])
                results[table] = data
            except Exception as e:
                print(f"Error processing {table}: {e}")
                results[table] = pd.DataFrame()
        
        return results

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

        self._embedding_model = None
        self._qdrant_client = None
        
        # UI Components
        self._init_ui_components()

    async def _get_embedding_model(self):
        """Lazy loading cho embedding model"""
        
        if self._embedding_model is None:
            from sentence_transformers import SentenceTransformer
            self._embedding_model = await asyncio.to_thread(
                SentenceTransformer, "thenlper/gte-base"
            )
        return self._embedding_model
    
    async def _get_qdrant_client(self):
        """Lazy loading cho Qdrant client"""
        if self._qdrant_client is None:
            import qdrant_client
            self._qdrant_client = await asyncio.to_thread(qdrant_client.QdrantClient(path="./qdrant_data"))
            
        return self._qdrant_client
    
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
    
    async def _init_embedding_async(self):
        """Async initialization cho embedding model"""
        if not self._embedding_initialized:
            await asyncio.to_thread(self._init_embedding_sync)
            self._embedding_initialized = True
    
    def _init_embedding_sync(self):
        """Sync initialization cho embedding (chạy trong thread pool)"""
        try:
            model = get_embedding_model()
            qdrant = get_qdrant_client()
            
            # Check collection exists
            collections = qdrant.get_collections().collections
            if COLLECTION_NAME not in [c.name for c in collections]:
                from qdrant_client.models import Distance, VectorParams
                qdrant.recreate_collection(
                    collection_name=COLLECTION_NAME,
                    vectors_config=VectorParams(size=768, distance=Distance.COSINE),
                )
        except Exception as e:
            print(f"Embedding initialization error: {e}")
    
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
        """Improved AI response với async processing"""
        try:
            # Normalize query
            normalized_query = self.preprocess_question(message_text)
            
            # Extract codes first
            codes = self.query_engine.extract_codes(message_text)
            # Score different functions
            function_scores = self.query_engine.score_query_match(message_text, normalized_query)
            
            if codes:
                return await self._handle_code_query(codes, function_scores)
            elif function_scores:
                return await self._handle_function_query(function_scores, message_text)
            else:
                return await self._handle_general_query(message_text)
                
        except Exception as e:
            print(f"AI Response error: {e}")
            return f"❌ Có lỗi xảy ra khi xử lý câu hỏi: {str(e)}"
    
    async def _handle_code_query(self, codes: List[str], function_scores: Dict[str, float]) -> Any:
        """Xử lý query có chứa mã codes"""
        codes_str = ",".join(f"'{code}'" for code in codes)
        
        # Determine which tables to query based on function scores
        tables_to_query = []
        
        if function_scores:
            # Query specific functions
            top_function = max(function_scores.items(), key=lambda x: x[1])[0]
            if top_function in FUNCTION_MAPPING:
                module, func = FUNCTION_MAPPING[top_function]
                print(f"Querying {module}.{func}")
                table_name = self._get_table_name(module, func)
                condition = self._build_condition(table_name, codes_str)
                tables_to_query.append((table_name, "*", condition))
        else:
            # Query all relevant tables
            main_tables = [
                ("dm_technical", "*", f'"SC_NO" IN ({codes_str})'),
                ("dm_actual", "*", f'"SC_NO" IN ({codes_str})'),
                ("process_wip", "*", f'"SC_NO" IN ({codes_str})')
            ]
            tables_to_query.extend(main_tables)
        
        # Execute queries
        results = await self.query_engine.process_multiple_queries(tables_to_query)
        
        return self._format_results(results, codes)
    
    async def _handle_function_query(self, function_scores: Dict[str, float], message_text: str) -> Any:
        """Xử lý query theo function"""
        # Get top function
        top_function = max(function_scores.items(), key=lambda x: x[1])[0]
        
        if top_function in FUNCTION_MAPPING:
            module, func = FUNCTION_MAPPING[top_function]
            table_name = self._get_table_name(module, func)
            
            data = await self.query_engine.get_data_async(table_name, "*")
            
            if not data.empty:
                # Limit to first 10 rows for display
                display_data = data.head(10)
                table = self._create_data_table(display_data)
                self.last_data = data
                
                return (f"📊 Kết quả cho '{func}' ({len(data)} bản ghi):", table)
            else:
                return f"❌ Không tìm thấy dữ liệu cho '{func}'"
        
        return "❌ Không tìm thấy chức năng phù hợp"
    
    async def _handle_general_query(self, message_text: str) -> str:
        """Xử lý general query"""
        # Initialize embedding if needed
        if not self._embedding_initialized:
            await self._init_embedding_async()
        
        # Search suggestions
        suggestions = await self._search_suggestions(message_text)
        
        if suggestions:
            suggestion_text = "\n".join([f"👉 {s}" for s in suggestions])
            return f"💡 Có thể bạn muốn hỏi:\n{suggestion_text}"
        else:
            return ("❓ Tôi chưa hiểu rõ câu hỏi. Hãy thử:\n"
                   "• 'Xem DM Technical cho SC1234'\n"
                   "• 'Báo cáo Process WIP'\n"
                   "• 'So sánh DM Technical và Actual'")
    
    async def _search_suggestions(self, query: str) -> List[str]:
        """Tìm kiếm gợi ý từ embedding"""
        try:
            model = get_embedding_model()
            qdrant = get_qdrant_client()
            
            # Encode query
            embedding = await asyncio.to_thread(model.encode, query)
            
            # Search
            hits = await asyncio.to_thread(
                qdrant.search,
                collection_name=COLLECTION_NAME,
                query_vector=embedding.tolist(),
                limit=3
            )
            
            return [hit.payload.get("desc", "") for hit in hits if hit.score > 0.7]
            
        except Exception as e:
            print(f"Search error: {e}")
            return []
    
    def _get_table_name(self, module: str, func: str) -> str:
        """Map module/function to table name"""
        mapping = {
            ("DM Technical", "Report Technical"): "dm_technical",
            ("DM Actual", "Report Actual"): "dm_actual",
            ("DM Technical", "List GO"): "list_go",
            ("DM Technical", "Cutting Forecast"): "cutting_forecast",
            ("DM Technical", "Go Quantity"): "go_quantity",
            ("DM Technical", "Submat Demand"): "submat_demand",
            ("DM Technical", "Master Fabric List"): "fabric_list",
            ("DM Technical", "Master Trims List"): "trims_list",
            ("DM Actual", "Fabric Trans Summary"): "fabric_trans",
            ("DM Actual", "JO Process Wip"): "process_wip",
            ("DM Actual", "Submat Trans Summary"): "submat_trans",
        }
        return mapping.get((module, func), "dm_technical")
    
    def _build_condition(self, table_name: str, codes_str: str) -> str:
        """Build SQL condition based on table"""
        code_column_mapping = {
            "dm_technical": "SC_NO", #ok
            "dm_actual": "SC_NO", #ok
            "process_wip": "SC_NO", # JO_NO
            "cutting_forecast": "GO", # JO
            "go_quantity": "GO_No", # ok
            "submat_demand": "GO", # JO_NO
            "fabric_trans": "SC_NO", # JO_NO
            "submat_trans": "SC_NO" # JO_NO
        }
        
        column = code_column_mapping.get(table_name, "SC_NO")
        return f'"{column}" IN ({codes_str})'
    
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
    
    # UI Methods
    def add_welcome_message(self):
        """Add welcome message"""
        welcome_text = """👋 Xin chào! Tôi là AI Assistant.

💡 Bạn có thể hỏi tôi:
• "Xem DM Technical cho SC1234"
• "Báo cáo Process WIP"  
• "So sánh DM Technical và Actual"
• "Danh sách GO"

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
        """Remove typing indicator"""
        if typing_container in self.chat_container.controls:
            self.chat_container.controls.remove(typing_container)
            self.page.update()
    
    async def send_message(self, e=None):
        """Send message"""
        message_text = self.input_field.value.strip()
        if not message_text:
            return
        
        # Clear input
        self.input_field.value = ""
        self.page.update()

        # Warm up nếu cần
        if not getattr(self, "_warmed_up", False):
            await self.query_engine._warm_up_connections()
            self._warmed_up = True

        # Add user message
        user_message = ChatMessage("user", message_text, is_user=True)
        self.messages.append(user_message)
        self.display_message(user_message)
        
        # Show typing indicator
        typing_indicator = self.show_typing_indicator()
        
        try:
            # Get AI response
            ai_response = await self.get_ai_response(message_text)
            self.remove_typing_indicator(typing_indicator)
            
            if isinstance(ai_response, list):
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
        file_name = "ket_qua.xlsx"
        # print(self.last_data, self.func_name.split("'")[1])
        # Lưu dữ liệu excel vào biến tạm
        if hasattr(self, "last_data") and self.last_data is not None:
            excel_bytes = self.to_excel(self.last_data)
            self._excel_bytes = excel_bytes
            self.file_picker.on_result = self.save_excel
            self.file_picker.save_file(file_name=f"{file_name}")
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
        

