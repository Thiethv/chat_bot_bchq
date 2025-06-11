import flet as ft
import bcrypt
from database.connect_supabase import SupabaseFunctions

supa_func = SupabaseFunctions()

def register_user():
    username_input = ft.TextField(label="Tên đăng nhập mới", autofocus=True)
    password_input = ft.TextField(label="Mật khẩu", password=True, can_reveal_password=True)
    role_dropdown = ft.Dropdown(
        label="Vai trò",
        options=[
            ft.dropdown.Option("user"),
            ft.dropdown.Option("admin"),
        ],
        value="user"
    )
    message_text = ft.Text("")

    def submit_register(e):
        username = username_input.value
        password = password_input.value
        role = role_dropdown.value

        if not username or not password:
            message_text.value = "❌ Vui lòng nhập đầy đủ thông tin."
            message_text.color = "red"
        else:
            hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
            response = supa_func.create_user(username, hashed_pw, role)
            if response and response.data:
                message_text.value = "✅ Đăng ký người dùng thành công!"
                message_text.color = "green"
                username_input.value = ""
                password_input.value = ""
            else:
                message_text.value = f"❌ Lỗi đăng ký: {response}"
                message_text.color = "red"
        e.page.update()

    return ft.Column(
        controls=[
            ft.Text("Đăng ký người dùng mới", size=20, weight=ft.FontWeight.BOLD),
            username_input,
            password_input,
            role_dropdown,
            ft.ElevatedButton("Thêm người dùng", icon=ft.Icons.PERSON_ADD, on_click=submit_register),
            message_text
        ],
        spacing=15
    )
