import flet as ft
from ui_setup.pages.login_page import LoginPage
from ui_setup.main_page import MainPage

class FletApp:
    def __init__(self):
        self.page = None
        self.is_logged_in = False
        self.current_user = ""
        self.sidebar_open = True
        self.selected_files = []
        self.current_page = "main"  # main, technical, actual

        self.selected_rows = set()
        
    def main(self, page: ft.Page):
        self.page = page
        page.title = "IE Assistant"
        # page.window_maximized = True
        page.window_full_screen = True
        page.theme_mode = ft.ThemeMode.LIGHT

        def on_login_success(username):
            self.is_logged_in = True
            self.current_user = username
            self.current_page = "main"
            MainPage(self.page, self.sidebar_open, self.current_user, self.current_page, on_login_success=on_login_success).show_main_app()

        login_page = LoginPage(self.page, on_login_success=on_login_success)
        login_page.show_login_page()
       
def main(page: ft.Page):
    app = FletApp()
    app.main(page)

if __name__ == "__main__":
    ft.app(target=main)
