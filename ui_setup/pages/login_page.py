import flet as ft
import bcrypt
from database.connect_supabase import SupabaseFunctions

class LoginPage:
    def __init__(self, page: ft.Page, on_login_success=None):
        self.page = page
        self.username_field = None
        self.password_field = None
        self.message_text = None

        self.on_login_success = on_login_success

        self.supa_func = SupabaseFunctions()
        

    def show_login_page(self):
        """Hiển thị trang đăng nhập"""
        self.username_field = ft.TextField(
            label="Tên đăng nhập",
            width=300,
            prefix_icon=ft.Icons.PERSON
        )
        
        self.password_field = ft.TextField(
            label="Mật khẩu",
            width=300,
            password=True,
            can_reveal_password=True,
            prefix_icon=ft.Icons.LOCK
        )

        self.message_text = ft.Text("")

        login_button = ft.ElevatedButton(
            text="Đăng nhập",
            width=300,
            height=40,
            on_click = self.handle_login
        )

        register_button = ft.ElevatedButton(
            text="Đăng ký người dùng",
            width=300,
            height=40,
            # on_click = self.show_register_page
        )

        self.is_logged_in = False
        self.current_page = "main"
        self.current_user = ""
        
        login_container = ft.Container(
            content=ft.Column(
                controls=[
                    ft.Text("🔐 Đăng nhập hệ thống", size=24, weight=ft.FontWeight.BOLD),
                    ft.Container(height=20),
                    self.username_field,
                    self.password_field,
                    self.message_text,
                    ft.Container(height=20),
                    login_button,
                    register_button
                ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=10
            ),
            alignment=ft.alignment.center,
            width=400,
            height=300,
            bgcolor=ft.Colors.WHITE,
            border_radius=10,
            shadow=ft.BoxShadow(
                spread_radius=1,
                blur_radius=15,
                color=ft.Colors.BLUE_GREY_300,
                offset=ft.Offset(0, 0),
            )
        )
        
        self.page.controls.clear()
        self.page.add(
            ft.Container(
                content=login_container,
                alignment=ft.alignment.center,
                expand=True,
                bgcolor=ft.Colors.BLUE_50
            )
        )
        self.page.update()
    
    def handle_login(self, e):
        """Xử lý đăng nhập"""
        username = self.username_field.value
        password = self.password_field.value
        
        # Kiểm tra đăng nhập đơn giản (có thể thay thế bằng logic thật)
        if not username or not password:
            self.message_text.value = "❌ Vui lòng nhập đầy đủ thông tin."
            self.message_text.color = "red"

        else:
            response = self.supa_func.get_user_by_username(username)
            if response and response.data:
                user = response.data[0]
                if bcrypt.checkpw(password.encode(), user['password'].encode()):
                    self.is_logged_in = True
                    self.current_user = username
                    self.current_page = "main"

                    if self.on_login_success:
                        self.on_login_success(self.current_user)
                    return
                    
                else:
                    self.message_text.value = "❌ Sai mật khẩu!"
                    self.message_text.color = "red"
            else:
                self.message_text.value = "❌ Người dùng không tồn tại!"
                self.message_text.color = "red"
     
        e.page.update()