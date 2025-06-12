import flet as ft
from datetime import datetime
from ui_setup.pages.login_page import LoginPage
from ui_setup.pages.chat_page import ChatPage

class MainPage:
    def __init__(
        self,
        page: ft.Page,
        sidebar_open=None,
        current_user=None,
        current_page=None,
        on_login_success=None
    ):
        self.page = page
        self.sidebar_open = sidebar_open
        self.current_user = current_user
        self.current_page = current_page
        self.on_login_success = on_login_success

        self.report_page = ChatPage(self.page)

        # NEW: Lưu nhiều cuộc hội thoại (mỗi cuộc là dict {'name', 'messages'})
        self.chat_histories = []
        self.current_chat_name = None

    def show_main_app(self):
        """Hiển thị ứng dụng chính với sidebar và content tương ứng"""
        self.page.controls.clear()

        # Tạo sidebar
        sidebar = self.create_sidebar()

        # Tạo nội dung tương ứng với trang hiện tại
        if self.current_page == "main":
            main_content = self.report_page.create_main_content()
        # Thêm trang lịch sử chat nếu muốn
        elif self.current_page == "history_chat":
            main_content = self.create_history_content()
            self.report_page.welcome_shown = False
        else:
            main_content = self.report_page.create_main_content()

        # Layout chính
        main_layout = ft.Row(
            controls=[
                sidebar,
                ft.Container(
                    content=main_content,
                    expand=True,
                    padding=20,
                    bgcolor=ft.Colors.GREY_50
                )
            ],
            spacing=0,
            expand=True
        )

        self.page.add(main_layout)
        self.page.update()

    def create_sidebar(self):
        """Tạo sidebar"""
        # Toggle button cho sidebar
        toggle_button = ft.IconButton(
            icon=ft.Icons.MENU,
            on_click=self.toggle_sidebar,
            tooltip="Đóng/Mở Menu"
        )

        # Navigation buttons với Chat mới
        nav_buttons = ft.Column(
            controls=[
                ft.ElevatedButton(
                    text="Chat mới",
                    icon=ft.Icons.ADD,
                    on_click=self.handle_new_chat,
                    bgcolor=ft.Colors.BLUE_100,
                    color=ft.Colors.BLUE_900,
                    width=200
                ),
                
                ft.ElevatedButton(
                    text="Lịch sử chat",
                    icon=ft.Icons.ENGINEERING,
                    on_click=lambda e: self.navigate_to("history_chat"),
                    bgcolor=ft.Colors.BLUE_600 if self.current_page == "history_chat" else ft.Colors.GREY_300,
                    color=ft.Colors.WHITE if self.current_page == "history_chat" else ft.Colors.BLACK,
                    width=200
                ),
            ],
            spacing=10
        )

        # Thông tin người dùng
        user_info = ft.Column(
            controls=[
                ft.Container(
                    content=ft.Row(
                        controls=[
                            ft.Icon(ft.Icons.ACCOUNT_CIRCLE, size=40, color=ft.Colors.BLUE),
                            ft.Text(f"Xin chào, {self.current_user}", size=16, weight=ft.FontWeight.BOLD)
                        ],
                        alignment=ft.MainAxisAlignment.START
                    ),
                    padding=10
                ),
                ft.Divider(),
                ft.Container(
                    content=nav_buttons,
                    padding=15
                ),
                ft.Divider(),
                ft.Container(
                    content=ft.ElevatedButton(
                        text="Đăng xuất",
                        icon=ft.Icons.LOGOUT,
                        on_click=self.handle_logout,
                        bgcolor=ft.Colors.RED_400,
                        color=ft.Colors.WHITE,
                        width=200
                    ),
                    padding=15
                ),
                # Hiển thị danh sách lịch sử chat ngay trong sidebar (tuỳ chọn)
                self.render_sidebar_chat_history()
            ],
            spacing=5
        )

        sidebar_content = ft.Column(
            controls=[
                ft.Container(
                    content=toggle_button,
                    padding=10,
                    alignment=ft.alignment.top_right
                ),
                user_info if self.sidebar_open else ft.Container()
            ],
            spacing=0
        )

        return ft.Container(
            content=sidebar_content,
            width=250 if self.sidebar_open else 60,
            bgcolor=ft.Colors.WHITE,
            border=ft.border.only(right=ft.border.BorderSide(1, ft.Colors.GREY_300)),
            animate=ft.Animation(300, ft.AnimationCurve.EASE_IN_OUT)
        )

    def render_sidebar_chat_history(self):
        """Hiển thị danh sách lịch sử chat ở sidebar (tuỳ chọn)"""
        if not self.chat_histories:
            return ft.Container()
        # Chỉ hiện tối đa 10 cuộc chat gần nhất
        controls = [
            ft.Text("Lịch sử chat gần đây:", size=12, color=ft.Colors.GREY_600, italic=True)
        ]
        for idx, chat in enumerate(reversed(self.chat_histories[-10:])):
            controls.append(
                ft.TextButton(
                    chat["name"],
                    on_click=lambda e, i=len(self.chat_histories)-1-idx: self.load_chat_history(i),
                    style=ft.ButtonStyle(color=ft.Colors.BLUE_700),
                    tooltip="Mở lại hội thoại này"
                )
            )
        return ft.Container(
            content=ft.Column(controls=controls, spacing=2),
            padding=ft.padding.only(left=15, right=15, bottom=10)
        )

    def handle_new_chat(self, e=None):
        # CHỐT: chỉ lưu nếu khác với cuộc chat cuối cùng
        if self.report_page.messages and any(m.is_user for m in self.report_page.messages):
            need_save = True
            if self.chat_histories:
                last_history_msgs = self.chat_histories[-1]["messages"]
                # So sánh từng message (nội dung và vai trò)
                if len(last_history_msgs) == len(self.report_page.messages):
                    all_same = all(
                        (m.text == h.text and m.is_user == h.is_user)
                        for m, h in zip(self.report_page.messages, last_history_msgs)
                    )
                    if all_same:
                        need_save = False
            if need_save:
                user_msgs = [m for m in self.report_page.messages if m.is_user]
                if user_msgs:
                    chat_name = user_msgs[0].text[:30]
                else:
                    from datetime import datetime
                    chat_name = "Chat lúc " + datetime.now().strftime("%H:%M %d/%m")
                # Lưu bản copy, không phải tham chiếu!
                self.chat_histories.append({
                    "name": chat_name,
                    "messages": [m.clone() if hasattr(m, "clone") else m for m in self.report_page.messages]
                })
        # Reset lại chat hiện tại
        self.report_page.messages.clear()
        self.report_page.chat_container.controls.clear()
        self.report_page.data_history.clear()
        self.report_page.last_data = None
        self.report_page.uploaded_file_data = None
        self.report_page.welcome_shown = False
        self.report_page.add_welcome_message()

        # --- SỬA ở đây ---
        # Điều hướng về trang chat chính và render lại giao diện + sidebar
        self.current_page = "main"
        self.show_main_app()

    def load_chat_history(self, idx):
        """Tải lại 1 cuộc chat trong lịch sử"""
        if 0 <= idx < len(self.chat_histories):
            history = self.chat_histories[idx]
            # LUÔN dùng bản copy, không tham chiếu
            self.report_page.messages = [m.clone() if hasattr(m, "clone") else m for m in history["messages"]]
            # Clear UI chat container rồi render lại từng message
            self.report_page.chat_container.controls.clear()
            for msg in self.report_page.messages:
                self.report_page.display_message(msg)
            self.page.update()

    def create_history_content(self):
        """Trang lịch sử chat: liệt kê các cuộc chat, bấm vào để mở lại"""
        if not self.chat_histories:
            return ft.Text("Chưa có lịch sử chat", size=16, color=ft.Colors.GREY_500)
        controls = []
        for idx, chat in enumerate(reversed(self.chat_histories)):
            controls.append(
                ft.ListTile(
                    title=ft.Text(chat["name"], size=15, weight=ft.FontWeight.BOLD),
                    subtitle=ft.Text(f"{len(chat['messages'])} tin nhắn"),
                    on_click=lambda e, i=len(self.chat_histories)-1-idx: self.load_chat_history(i),
                    leading=ft.Icon(ft.Icons.CHAT_BUBBLE_OUTLINE),
                )
            )
        return ft.Column(controls=controls, scroll=ft.ScrollMode.AUTO, expand=True)

    def navigate_to(self, page_name):
        """Điều hướng đến trang khác"""
        self.current_page = page_name
        self.show_main_app()

    def toggle_sidebar(self, e):
        """Đóng/mở sidebar"""
        self.sidebar_open = not self.sidebar_open
        self.show_main_app()

    def handle_logout(self, e):
        """Xử lý đăng xuất"""
        self.is_logged_in = False
        self.current_user = ""
        self.sidebar_open = True
        self.current_page = "main"
        LoginPage(self.page, on_login_success=self.on_login_success).show_login_page()