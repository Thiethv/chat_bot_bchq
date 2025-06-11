# ui_setup/utils/login.py

import flet as ft
import bcrypt
from database.connect_supabase import SupabaseFunctions

supa_func = SupabaseFunctions()

def login_user_to_session(session, username, role, remember):
    session["logged_in"] = True
    session["username"] = username
    session["role"] = role
    session["remember"] = remember

def login(session, on_success_callback=None):
    username_input = ft.TextField(label="Tên đăng nhập")
    password_input = ft.TextField(label="Mật khẩu", password=True, can_reveal_password=True)
    remember_checkbox = ft.Checkbox(label="Lưu tên đăng nhập")
    message_text = ft.Text("")

    # Nếu có nhớ user trước đó thì auto điền
    if session.get("remember") and session.get("username"):
        username_input.value = session["username"]
        remember_checkbox.value = True

    def submit_login(e):
        username = username_input.value
        password = password_input.value
        remember = remember_checkbox.value

        if not username or not password:
            message_text.value = "❌ Vui lòng nhập đầy đủ thông tin."
            message_text.color = "red"
        else:
            response = supa_func.get_user_by_username(username)
            if response and response.data:
                user = response.data[0]
                if bcrypt.checkpw(password.encode(), user['password'].encode()):
                    # Cập nhật session
                    login_user_to_session(session, username, user['role'], remember)
                    message_text.value = "✅ Đăng nhập thành công!"
                    message_text.color = "green"
                    if callable(on_success_callback):
                        on_success_callback()
                else:
                    message_text.value = "❌ Sai mật khẩu!"
                    message_text.color = "red"
            else:
                message_text.value = "❌ Người dùng không tồn tại!"
                message_text.color = "red"
        e.page.update()

    return ft.Container(
        content=ft.Column(
            controls=[
                ft.Text("🔐 Đăng nhập hệ thống", size=20, weight=ft.FontWeight.BOLD),
                username_input,
                password_input,
                remember_checkbox,
                ft.ElevatedButton("Login", icon=ft.Icons.LOGIN, on_click=submit_login),
                message_text
            ],
            spacing=10
        ),
        padding=20,
        width=400
    )
