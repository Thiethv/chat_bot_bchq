from typing import List
import unicodedata

class DataProcessor:
    def __init__(self):
        self.history = []

    def normalize_codes(self, code_name: str) -> List[str]:
        """Chuẩn hóa danh sách mã GO/JO"""
        if len(self.history) >100:
            self.history.pop(0)
            
        self.history.append(("normalize", code_name))

        normalized_codes = []
        for code in code_name.split(","):
            code = code.strip()
            if len(code) > 9:
                normalized_codes.append(f"S{code[:8]}")
            else:
                normalized_codes.append(code)
        codes_str = ",".join(f"'{code}'" for code in normalized_codes)

        return codes_str

    def extract_codes(self, code_name: str) -> List[str]:
        code_list = []
        for code in code_name.split(","):
            code = code.strip()
            if len(code) == 9 and code.startswith("S"):
                code_list.append(code[1:])  # Bỏ ký tự đầu
            else:
                code_list.append(code)

        jo_nos_str = ",".join([f"'{code}'" for code in code_list])

        return jo_nos_str
    
    def normalize_text(self, text):
        """ Chuẩn hóa văn bản """
        text = text.lower().strip()
        text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        return text