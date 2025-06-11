import flet as ft
from ui_setup.pages.login_page import LoginPage
# from ui_setup.pages.report_page import ReportPage
from ui_setup.pages.chat_page import ChatPage

class MainPage:
    def __init__(self, page: ft.Page, sidebar_open = None, current_user = None, current_page = None, on_login_success=None):
        self.page = page
        self.sidebar_open = sidebar_open
        self.current_user = current_user
        self.current_page = current_page

        self.on_login_success = on_login_success
        self.report_page = ChatPage(self.page)

    def show_main_app(self):
        """Hiển thị ứng dụng chính với sidebar và content tương ứng"""
        self.page.controls.clear()
        
        # Tạo sidebar
        sidebar = self.create_sidebar()
        
        # Tạo nội dung tương ứng với trang hiện tại
        if self.current_page == "main":
            main_content = self.report_page.create_main_content()
        
        # if self.current_page == "history_chat":
        #     main_content = self.report_page.create_history_content()
        
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
        
        # Navigation buttons
        nav_buttons = ft.Column(
            controls=[
                ft.ElevatedButton(
                    text="Trang chính",
                    icon=ft.Icons.HOME,
                    on_click=lambda e: self.navigate_to("main"),
                    bgcolor=ft.Colors.BLUE_400 if self.current_page == "main" else ft.Colors.GREY_300,
                    color=ft.Colors.WHITE if self.current_page == "main" else ft.Colors.BLACK,
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
                )
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
        
    def on_dropdown_change(self, e):
        """Xử lý thay đổi dropdown"""
        print(f"Đã chọn: {e.control.value}")