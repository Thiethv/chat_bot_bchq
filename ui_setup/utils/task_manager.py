import asyncio
import re
from typing import List, Optional, Dict, Any

import pandas as pd

from database.connect_supabase import SupabaseFunctions
from ui_setup.utils.task_pattern import TaskPattern
from ui_setup.utils.data_processor import DataProcessor

class TaskCondition:
    """Định nghĩa điều kiện cho một tác vụ"""
    def __init__(self, name: str, description: str, validation_func, required: bool = True):
        self.name = name
        self.description = description
        self.validation_func = validation_func
        self.required = required

class TaskDefinition:
    """Định nghĩa một tác vụ với các điều kiện cần thiết"""
    def __init__(self, name: str, description: str, conditions: List[TaskCondition], execute_func):
        self.name = name
        self.description = description
        self.conditions = conditions
        self.execute_func = execute_func

class TaskManager:
    """Quản lý các tác vụ và validation điều kiện"""
    
    def __init__(self, query_engine=None):
        self.tasks = {}
        self._register_tasks()
        self.query_engine = query_engine
        self.processor = DataProcessor()

    def _register_tasks(self):
        """Đăng ký các tác vụ có sẵn"""
        
        # DM Technical Report
        self.tasks["dm_technical"] = TaskDefinition(
            name="DM Technical Report",
            description="Báo cáo định mức kỹ thuật (technical report)",
            conditions=[
                TaskCondition(
                    "codes", 
                    "Cần có mã SC_NO (VD: S24M12345)", 
                    lambda query: self._extract_codes(query),
                    required=False  # Có thể xem toàn bộ
                )
            ],
            execute_func=self._execute_dm_technical
        )
        
        # DM Actual Report  
        self.tasks["dm_actual"] = TaskDefinition(
            name="DM Actual Report",
            description="Báo cáo định mức thực tế (Actual report)",
            conditions=[
                TaskCondition(
                    "codes",
                    "Cần có mã SC_NO (VD: S24M12345)",
                    lambda query: self._extract_codes(query),
                    required=False
                )
            ],
            execute_func=self._execute_dm_actual
        )
        
        # Compare Report
        self.tasks["compare"] = TaskDefinition(
            name="Compare DM Report",
            description="Báo cáo so sánh định mức kỹ thuật và thực tế",
            conditions=[
                TaskCondition(
                    "codes",
                    "Cần có mã SC để so sánh (VD: S24M123456)",
                    lambda query: self._extract_codes(query),
                    required=False  # Bắt buộc có mã để so sánh
                )
            ],
            execute_func=self._execute_compare
        )
        
        # Process WIP
        self.tasks["process_wip"] = TaskDefinition(
            name="Process WIP Report", 
            description="Báo cáo process wip",
            conditions=[
                TaskCondition(
                    "codes",
                    "Cần có mã SC hoặc JO (VD: S24M123456, 24M12345AB01)",
                    lambda query: self._extract_codes(query),
                    required=False
                )
            ],
            execute_func=self._execute_process_wip
        )
        
        # Insert/Update Tasks
        self.tasks["insert_trims"] = TaskDefinition(
            name="Insert Trims List",
            description="Cập nhật danh sách master trims",
            conditions=[
                TaskCondition(
                    "file_data",
                    "Cần có file Excel chứa dữ liệu trims",
                    lambda query, context: context.get("file_data"),
                    required=False
                )
            ],
            execute_func=self._execute_insert_trims
        )
        
        self.tasks["insert_fabric"] = TaskDefinition(
            name="Insert Fabric List",
            description="Cập nhật danh sách master fabric",
            conditions=[
                TaskCondition(
                    "file_data",
                    "Cần có file Excel chứa dữ liệu fabric",
                    lambda query, context: context.get("file_data"),
                    required=False
                )
            ],
            execute_func=self._execute_insert_fabric
        )

        self.tasks["insert_range_dm"] = TaskDefinition(
            name="Insert Range DM",
            description="Cập nhật danh sách range định mức",
            conditions=[
                TaskCondition(
                    "file_data",
                    "Cần có file Excel chứa dữ liệu range định mức",
                    lambda query, context: context.get("file_data"),
                    required=False
                )
            ],
            execute_func=self._execute_insert_range_demand
        )

        # Cutting Forecast
        self.tasks["cutting_forecast"] = TaskDefinition(
            name="Cutting Forecast Report",
            description="Báo cáo Cutting forecast",
            conditions=[
                TaskCondition(
                    "codes",
                    "Cần có mã GO/JO (VD: S24M12345)",
                    lambda query: self._extract_codes(query),
                    required=False
                )
            ],
            execute_func=self._execute_cutting_forecast
        )

        # Fabric Transaction Summary
        self.tasks["fabric_trans"] = TaskDefinition(
            name="Fabric Transaction Summary",
            description="Tổng hợp fabric transaction",
            conditions=[
                TaskCondition(
                    "codes",
                    "Cần có mã GO/JO (VD: S24M12345)",
                    lambda query: self._extract_codes(query),
                    required=False
                )
            ],
            execute_func=self._execute_fabric_trans
        )
        
        # Submat Transaction Summary
        self.tasks["submat_trans"] = TaskDefinition(
            name="Submat Transaction Summary",
            description="Tổng hợp submat transaction",
            conditions=[
                TaskCondition(
                    "codes",
                    "Cần có mã GO/JO (VD: S24M12345)",
                    lambda query: self._extract_codes(query),
                    required=False
                )
            ],
            execute_func=self._execute_submat_trans
        )

        # Submat Demand
        self.tasks["submat_demand"] = TaskDefinition(
            name="Submat Demand Report",
            description="Xem báo cáo yêu cầu nguyên phụ liệu (submat demand)",
            conditions=[
                TaskCondition(
                    "codes",
                    "Cần có mã GO/JO (VD: S24M12345)",
                    lambda query: self._extract_codes(query),
                    required=False
                )
            ],
            execute_func=self._execute_submat_demand
        )

        # GO Quantity
        self.tasks["go_quantity"] = TaskDefinition(
            name="GO Quantity Report",
            description="Số lượng GO Quantity",
            conditions=[
                TaskCondition(
                    "codes",
                    "Cần có mã GO (VD: S24M12345)",
                    lambda query: self._extract_codes(query),
                    required=False
                )
            ],
            execute_func=self._execute_go_quantity
        )
        
        # Thêm các task khác tương tự...
    
    def _extract_codes(self, query: str) -> List[str]:
        """Extract codes từ query"""
        patterns = [
            r"s\d{2}m[a-z0-9]+",  # SC codes
            r"\d{2}m\d{5}[a-z]{2}\d{2}",  # JO codes
        ]
        
        codes = []
        for pattern in patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            codes.extend(matches)
        
        return list(set(codes))
    
    def is_no_sql_query(self, query: str) -> bool:
        """Kiem tra xem query co phai la query khong sql hay khong"""
        keywords = [
            r"xem",
            r"không.*?truy.*?vấn.*?sql",
            r"no.*?query.*?sql", 
            r"không.?online",
            r"offline",
            r"xem.*?(báo cáo|danh sách|dữ liệu)",
            f"xem.*?({', '.join(self.tasks.keys())})",
            ]
        
        return any(re.search(kw, query.lower(), re.IGNORECASE) for kw in keywords)
    
    def get_all_data(self, query: str) -> bool:
        """ Kiểm tra xem người dùng có muốn lấy tất cả dữ liệu về không """
        keywwords = [
            r"tất.*?cả",
            r"lấy.*?tất.*?cả",
            r"tất.*?cả.*?dữ liệu",
        ]
        return any(re.search(kw, query.lower(), re.IGNORECASE) for kw in keywwords)

    def validate_task(self, task_name: str, query: str, context: dict = None) -> dict:
        """Validate điều kiện cho task"""
        if task_name not in self.tasks:
            return {"valid": False, "error": "Task không tồn tại"}
        
        task = self.tasks[task_name]
        context = context or {}
        missing_conditions = []
        satisfied_conditions = {}
        
        for condition in task.conditions:
            try:
                # Gọi validation function
                if condition.validation_func.__code__.co_argcount > 1:
                    result = condition.validation_func(query, context)
                else:
                    result = condition.validation_func(query)
                
                if result:
                    satisfied_conditions[condition.name] = result
                elif condition.required:
                    missing_conditions.append(condition)
                    
            except Exception as e:
                if condition.required:
                    missing_conditions.append(condition)
        
        return {
            "valid": len(missing_conditions) == 0,
            "missing_conditions": missing_conditions,
            "satisfied_conditions": satisfied_conditions,
            "task": task
        }
    
    async def execute_task(self, task_name: str, conditions: dict, query_engine, context = None) -> Any:
        """Thực thi task với các điều kiện đã được validate"""
        if task_name not in self.tasks:
            raise ValueError(f"Task {task_name} không tồn tại")
        
        task = self.tasks[task_name]
        return await task.execute_func(conditions, query_engine, context)
    
    # Task execution functions
    async def _execute_dm_technical(self, conditions: dict, query_engine, context = None) -> Any:
        """Thực thi DM Technical task"""
        
        try:
            all_results = {}
            add_process = context.get("add_process_message") if context else None
            codes = conditions.get("codes", [])
            query = context.get("query") if context else ""

            if codes:

                from ui_setup.components.dm_technical import DemandTechnical
                code_name = ",".join(codes)
                codes_str = self.processor.normalize_codes(code_name)

                codes_sub = ",".join(f"'{code}'" for code in codes)
                
                def run_technical_report():
                    """Chạy TechnicalReport trong thread riêng"""
                    ds = DemandTechnical(code_name=codes_str)
                    ds.get_results_dm_technical()

                condition = f'"SC_NO" IN ({codes_str})'
                conditions_cf = f'"GO" IN ({codes_sub}) OR "JO" IN ({codes_sub})'
                conditions_sd = f'"GO" IN ({codes_sub}) OR "JO_NO" IN ({codes_sub})'

                if self.is_no_sql_query(query):
                    if add_process:
                        add_process("📥 Đang xem dữ liệu offline. Nếu cần cập nhật online hãy đổi câu hỏi thành \"Lấy báo cáo hoặc dữ liệu ...\"")

                    data = await query_engine.get_data_async("dm_technical", "*", condition)
                    data_cf = await query_engine.get_data_async("cutting_forecast", "*", conditions_cf)
                    data_sd = await query_engine.get_data_async("submat_demand", "*", conditions_sd)
                else:
                    # 1. Chạy Cutting Forecast
                    if add_process:
                        add_process("🔄 Đang lấy dữ liệu Cutting Forecast...")

                    result_cutting = await self._execute_cutting_forecast(conditions, query_engine)
                    if result_cutting is False or (isinstance(result_cutting, pd.DataFrame) and result_cutting.empty):
                        return {"type": "error", "message": "❌ Lỗi khi kết nối hoặc không có dữ liệu Cutting Forecast."}
                    
                    # 2. Chạy Submat Demand
                    if add_process:
                        add_process("🔄 Đang lấy dữ liệu Submat Demand...")

                    result_submat = await self._execute_submat_demand(conditions, query_engine)
                    if result_submat is False or (isinstance(result_submat, pd.DataFrame) and result_submat.empty):
                        return {"type": "error", "message": "❌ Lỗi khi kết nối hoặc không có dữ liệu Submat Demand."}
                    # 3. Chạy Technical Report
                    if add_process:
                        add_process("🔄 Đang tổng hợp DM Technical Report...")

                    await asyncio.to_thread(run_technical_report)
                    data = await query_engine.get_data_async("dm_technical", "*", condition, limit=1000, offset=0)
                    data_cf = await query_engine.get_data_async("cutting_forecast", "*", conditions_cf)
                    data_sd = await query_engine.get_data_async("submat_demand", "*", conditions_sd)
                
            else:
                if add_process:
                    add_process("📥 Không có mã cụ thể, đang lấy toàn bộ dữ liệu demand technical...")

                data = await query_engine.get_data_async("dm_technical", "*")
                data_cf = await query_engine.get_data_async("cutting_forecast", "*")
                data_sd = await query_engine.get_data_async("submat_demand", "*")

            all_results["submat_demand"] = data_sd
            all_results["cutting_forecast"] = data_cf
            all_results["dm_technical"] = data
                
            return all_results  # Trả về dict chứa tất cả kết quả
            
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi Technical Report: {e}"}        
    
    async def _execute_dm_actual(self, conditions: dict, query_engine, context = None) -> Any:
        """Thực thi DM Actual task"""
        all_results = {}      
        # 4. Chạy dm_actual
        try:
            add_process = context.get("add_process_message") if context else None
            query = context.get("query") if context else ""
            codes = conditions.get("codes", [])
            if codes:
                from ui_setup.components.dm_actual import DmActual
                code_name = ",".join(codes)
                codes_str = self.processor.normalize_codes(code_name)

                codes_sub = ",".join(f"'{code}'" for code in codes)

                def run_dm_actual():
                    """Chạy DmActual trong thread riêng"""
                    ds = DmActual(code_name=code_name)
                    ds.update_note_actual()

                condition = f'"SC_NO" IN ({codes_str})'
                condition_fb = f'"SC_NO" IN ({codes_sub}) OR "JO_NO" IN ({codes_sub})'
                condition_sm = f'"SC_NO" IN ({codes_sub}) OR "JO_NO" IN ({codes_sub})'
                condition_wip = f'"SC_NO" IN ({codes_sub}) OR "JO_NO" IN ({codes_sub})'

                if self.is_no_sql_query(query):
                    if add_process:
                        add_process("📥 Đang xem dữ liệu offline. Nếu cần cập nhật online hãy đổi câu hỏi thành \"Lấy báo cáo hoặc dữ liệu ...\"")
                    data = await query_engine.get_data_async("dm_actual", "*", condition)
                    data_fb = await query_engine.get_data_async("fabric_trans", "*", condition_fb)
                    data_sm = await query_engine.get_data_async("submat_trans", "*", condition_sm)
                    data_wip = await query_engine.get_data_async("process_wip", "*", condition_wip)

                else:
                    # 1. Chạy fabric trans
                    if add_process:
                        add_process("🔄 Đang chạy Fabric Transaction Summary...")

                    result_fabric = await self._execute_fabric_trans(conditions, query_engine)
                    if result_fabric is False or (isinstance(result_fabric, pd.DataFrame) and result_fabric.empty):
                        return {"type": "error", "message": "❌ Lỗi khi kết nối hoặc không có dữ liệu Fabric Transaction Summary."}
                    
                    # 2. Chạy submat trans
                    if add_process:
                        add_process("🔄 Đang chạy Submat Transaction Summary...")

                    result_submat = await self._execute_submat_trans(conditions, query_engine)
                    
                    if result_submat is False or (isinstance(result_submat, pd.DataFrame) and result_submat.empty):
                        return {"type": "error", "message": "❌ Lỗi khi kết nối hoặc không có dữ liệu Submat Transaction Summary."}                    
                    
                    # 3. Chạy process wip
                    if add_process:
                        add_process("🔄 Đang chạy Process WIP...")

                    result_process_wip = await self._execute_process_wip(conditions, query_engine)
                    if result_process_wip is False or (isinstance(result_process_wip, pd.DataFrame) and result_process_wip.empty):
                        return {"type": "error", "message": "❌ Lỗi khi kết nối hoặc không có dữ liệu Process WIP."}
                    
                    if add_process:
                        add_process("🔄 Đang chạy tổng hợp Actual Report...")
                    
                    await asyncio.to_thread(run_dm_actual)

                    data = await query_engine.get_data_async("dm_actual", "*", condition)
                    data_fb = await query_engine.get_data_async("fabric_trans", "*", condition_fb)
                    data_sm = await query_engine.get_data_async("submat_trans", "*", condition_sm)
                    data_wip = await query_engine.get_data_async("process_wip", "*", condition_wip)
            else:
                if add_process:
                    add_process("📥 Không có mã cụ thể, đang lấy toàn bộ dữ liệu định mức thức tế...")
                data = await query_engine.get_data_async("dm_actual", "*")
                data_fb = await query_engine.get_data_async("fabric_trans", "*")
                data_sm = await query_engine.get_data_async("submat_trans", "*")
                data_wip = await query_engine.get_data_async("process_wip", "*")

            all_results["dm_actual"] = data
            all_results["fabric_trans"] = data_fb
            all_results["submat_trans"] = data_sm
            all_results["process_wip"] = data_wip

            return all_results
        
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi DM Actual: {e}"}
    
    async def _execute_compare(self, conditions: dict, query_engine, context = None) -> Any:
        """Thực thi Compare task"""
        try:
            add_process = context.get("add_process_message") if context else None
            query = context.get("query") if context else ''
            codes = conditions.get("codes", [])
            code_name = None

            if codes and add_process and not self.is_no_sql_query(query):
                code_name = ",".join(codes)

                # 1. Chạy demand technicl
                add_process("📥 Đang lấy dữ liệu demand technical...")
                result_dm_technical = await self._execute_dm_technical(conditions, query_engine)
                if isinstance(result_dm_technical, dict) and result_dm_technical.get("type") == "error":
                    return result_dm_technical
                
                # 2. Chạy demand submat
                add_process("📥 Đang lấy dữ liệu demand submat...")
                result_dm_submat = await self._execute_dm_actual(conditions, query_engine)
                if isinstance(result_dm_submat, dict) and result_dm_submat.get("type") == "error":
                    return result_dm_submat
            else:
                add_process("📥 Dữ liệu offline demand technical và demand submat...")
            # 3. Chạy report compare
            if add_process:
                add_process("📥 Đang phân tích và lấy dữ liệu report compare...")
            from ui_setup.components.compare_report import ReportCompare
            
            def run_report_compare():
                """Chạy ReportCompare trong thread riêng"""
                rc = ReportCompare(code_name=code_name)
                return rc.process_compare()

            result = await asyncio.to_thread(run_report_compare)
            if isinstance(result, pd.DataFrame) or not result:
                return {"type": "error", "message": "❌ Không thể tạo báo cáo so sánh. Dữ liệu trống hoặc có lỗi."}

            return result
        
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi Report Compare: {e}"}
    
    async def _execute_process_wip(self, conditions: dict, query_engine,context = None) -> Any:
        """Thực thi Process WIP task"""
        try:
            add_process = context.get("add_process_message") if context else None
            query = context.get("query") if context else ''
            codes = conditions.get("codes", [])
            if codes:
                
                from ui_setup.data_dmtt.jo_process_wip import JoProcessWip

                codes_str = ",".join(f"'{code}'" for code in codes)
                code_name = ",".join(codes)
                jo_nos_str = self.processor.extract_codes(code_name)

                condition = f'"SC_NO" IN ({codes_str}) OR "JO_NO" IN ({codes_str})'

                def run_jo_process_wip():
                    """Chạy JoProcessWip trong thread riêng"""
                    jpw = JoProcessWip(code_name=jo_nos_str)
                    jpw.process_wip()

                if self.is_no_sql_query(query):
                    if add_process:
                        add_process("📥 Đang xem dữ liệu offline. Nếu cần cập nhật online hãy đổi câu hỏi thành \"Lấy báo cáo hoặc lấy dữ liệu ...\"")
                    data = await query_engine.get_data_async("process_wip", "*", condition)
                else:
                    if add_process:
                        add_process("📥 Đang lấy dữ liệu process wip...")
                    # Import và chạy JoProcessWip
                    await asyncio.to_thread(run_jo_process_wip)
                    data = await query_engine.get_data_async("process_wip", "*", condition)
            else:
                data = await query_engine.get_data_async("process_wip", "*")
            
            return data
        
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi Process WIP: {e}"}
    
    async def _execute_insert_trims(self, conditions: dict, query_engine, context = None) -> Any:
        """Thực thi Insert Trims task"""
        try:
            file_data = conditions.get("file_data")
            add_process = context.get("add_process_message") if context else None
            if not file_data:
                raise ValueError("Không có dữ liệu file")
            
            def run_insert_trims():
                from ui_setup.data_dmkt.data_master_list import MasterList
                ml = MasterList(file_data)
                ml.insert_list_trims_to_supabase()
                
            await asyncio.to_thread(run_insert_trims)

            if add_process:
                add_process("📊 Đã cập nhật dữ liệu master trims list thành công")
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi Insert Trims: {e}"}
    
    async def _execute_insert_fabric(self, conditions: dict, query_engine, context = None) -> Any:
        """Thực thi Insert Fabric task"""
        try:
            file_data = conditions.get("file_data")
            add_process = context.get("add_process_message") if context else None
            if not file_data:
                raise ValueError("Không có dữ liệu file")
            
            def run_insert_fabric():
                from ui_setup.data_dmkt.data_master_list import MasterList
                ml = MasterList(file_data)
                ml.insert_list_fabric_to_supabase()
                
            await asyncio.to_thread(run_insert_fabric)

            if add_process:
                add_process("📊 Đã cập nhật dữ liệu master fabric list")
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi Insert Fabric: {e}"}
    
    async def _execute_insert_range_demand(self, conditions: dict, query_engine, context = None) -> Any:
        """Thực thi Insert Range Demand task"""
        try:
            file_data = conditions.get("file_data")
            print("file_data:",file_data)
            add_process = context.get("add_process_message") if context else None
            if not file_data:
                raise ValueError("Không có dữ liệu file")
            
            def run_insert_range_demand():
                from ui_setup.data_dmkt.data_master_list import MasterList
                ml = MasterList(file_data)
                ml.insert_range_demand_to_supabase()
                
            await asyncio.to_thread(run_insert_range_demand)

            if add_process:
                add_process("📊 Đã cập nhật dữ liệu range demand")
        
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi Insert Range Demand: {e}"}

    async def _execute_cutting_forecast(self, conditions: dict, query_engine, context = None) -> Any: # Done
        """Thực thi Cutting Forecast task"""
        
        try:
            from ui_setup.data_dmkt.cutting_forecast import CuttingForecast
            add_process = context.get("add_process_message") if context else None
            query = context.get("query") if context else ""
            codes = conditions.get("codes", [])
            if codes:
                
                codes_str = ",".join(f"'{code}'" for code in codes)
                code_name = ",".join(codes)
                code_cut = self.processor.normalize_codes(code_name)

                # Import và chạy CuttingForecast
                def run_cutting_forecast():
                    """Chạy CuttingForecast trong thread riêng"""
                    cf = CuttingForecast(code_name=code_cut)
                    cf.into_supabase()

                condition = f'"GO" IN ({codes_str}) OR "JO" IN ({codes_str})'
                if self.is_no_sql_query(query):
                    if add_process:
                        add_process("📥 Đang xem dữ liệu offline. Nếu cần cập nhật online hãy đổi câu hỏi thành \"Lấy báo cáo hoặc lấy dữ liệu ...\"")
                    data = await query_engine.get_data_async("cutting_forecast", "*", condition)
                else:
                    if add_process:
                        add_process("📥 Đang lấy dữ liệu Cutting Forecast...")
                    await asyncio.to_thread(run_cutting_forecast)
                    data = await query_engine.get_data_async("cutting_forecast", "*", condition)
            else:
                data = await query_engine.get_data_async("cutting_forecast", "*")

            return data
        
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi Cutting Forecast: {e}"}

    async def _execute_fabric_trans(self, conditions: dict, query_engine, context = None) -> Any:
        """Thực thi Fabric Transaction Summary task"""
        try:
            add_process = context.get("add_process_message") if context else None
            query = context.get("query") if context else ''
            codes = conditions.get("codes", [])
            if codes:
                from ui_setup.data_dmtt.fabric_trans import FabricTrans
                codes_str = ",".join(f"'{code}'" for code in codes)

                def run_fabric_trans():
                    """Chạy FabricTrans trong thread riêng"""
                    ft = FabricTrans(code_name=codes_str)
                    ft.process_data()

                condition = f'"SC_NO" IN ({codes_str}) OR "JO_NO" IN ({codes_str})'
                if self.is_no_sql_query(query):
                    if add_process:
                        add_process("📥 Đang xem dữ liệu offline. Nếu cần cập nhật online hãy đổi câu hỏi thông \"Lấy báo cáo hoặc lấy dữ liệu ...\"")
                    data = await query_engine.get_data_async("fabric_trans", "*", condition)
                else:
                    if add_process:
                        add_process("📥 Đang lấy dữ liệu Fabric Transaction Summary...")
                    # Import và chạy FabricTrans
                    await asyncio.to_thread(run_fabric_trans)                           
                    data = await query_engine.get_data_async("fabric_trans", "*", condition)
            else:
                data = await query_engine.get_data_async("fabric_trans", "*")

            return data
        
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi Fabric Transaction Summary: {e}"}
    
    async def _execute_submat_trans(self, conditions: dict, query_engine, context = None) -> Any:
        """Thực thi Submat Transaction Summary task"""
        try:
            add_process = context.get("add_process_message") if context else None
            query = context.get("query") if context else ''
            codes = conditions.get("codes", [])
            if codes:
                from ui_setup.data_dmtt.submat_trans import SubmatTrans
                codes_str = ",".join(f"'{code}'" for code in codes)

                def run_submat_trans():
                    """Chạy SubmatTrans trong thread riêng"""
                    st = SubmatTrans(code_name=codes_str)
                    st.process_data()
                
                condition = f'"SC_NO" IN ({codes_str}) OR "JO_NO" IN ({codes_str})'
                if self.is_no_sql_query(query):
                    if add_process:
                        add_process("📥 Đang xem dữ liệu offline. Nếu cần cập nhật online hãy đổi câu hỏi thông \"Lấy báo cáo hoặc lấy dữ liệu ...\"")
                    data = await query_engine.get_data_async("submat_trans", "*", condition)
                else:
                    if add_process:
                        add_process("📥 Đang lấy dữ liệu Submat Transaction Summary...")
                    # Import và chạy SubmatTrans
                    await asyncio.to_thread(run_submat_trans)
                    data = await query_engine.get_data_async("submat_trans", "*", condition)
            else:
                data = await query_engine.get_data_async("submat_trans", "*")

            return data
        
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi Submat Transaction Summary: {e}"}

    async def _execute_submat_demand(self, conditions: dict, query_engine, context = None) -> Any: # Done
        """Thực thi Submat Demand task"""
        try:
            add_process = context.get("add_process_message") if context else None
            query = context.get("query") if context else ""
            codes = conditions.get("codes", [])
            if codes:
                
                from ui_setup.data_dmkt.get_dmsm_sql import DemandSM
                codes_str = ",".join(f"'{code}'" for code in codes)

                code_name = ",".join(codes)
                code_sd = self.processor.extract_codes(code_name)
                code_gq = self.processor.normalize_codes(code_name)

                def run_submat_demand():
                    """Chạy DemandSM trong thread riêng"""
                    ds = DemandSM(code_sd, code_gq)
                    ds.get_data_demand()
                    ds.get_go_quantity()

                condition = f'"JO_NO" IN ({codes_str}) OR "GO" IN ({codes_str})'
                if self.is_no_sql_query(query):
                    if add_process:
                        add_process("📥 Đang xem dữ liệu offline. Nếu cần cập nhật online hãy đổi câu hỏi thông \"Lấy báo cáo hoặc lấy dữ liệu ...\"")
                    data = await query_engine.get_data_async("submat_demand", "*", condition)
                    
                else:
                    if add_process:
                        add_process("📥 Đang lấy dữ liệu Submat Demand...")
                    # Import và chạy DemandSM
                    await asyncio.to_thread(run_submat_demand)
                    data = await query_engine.get_data_async("submat_demand", "*", condition)
            else:
                data = await query_engine.get_data_async("submat_demand", "*")
            return data
        
        except Exception as e:
            return {"type": "error", "message": f"❌ Lỗi khi thực thi Submat Demand: {e}"}

    async def _execute_go_quantity(self, conditions: dict, query_engine, context = None) -> Any:
        """Thực thi GO Quantity task"""
        codes = conditions.get("codes", [])
        if codes:
            codes_str = ",".join(f"'{code}'" for code in codes)
            
            condition = f'"GO_No" IN ({codes_str})'
            data = await query_engine.get_data_async("go_quantity", "*", condition)
        else:
            data = await query_engine.get_data_async("go_quantity", "*")
        return data
    
class AsyncQueryEngine:
    """Updated AsyncQueryEngine với TaskManager"""
    
    def __init__(self):
        self.supabase = SupabaseFunctions()
        self.task_manager = TaskManager(self)  # Thêm TaskManager
        self.task_patterns = TaskPattern()

    async def process_query_with_tasks(self, query: str, context: dict = None) -> dict:
        """Xử lý query với task-based approach"""
        # Identify task
        task_name = self.task_patterns.identify_task(query)
        
        if not task_name:
            return {
                "type": "no_task",
                "message": "Không thể xác định tác vụ. Vui lòng mô tả rõ hơn yêu cầu của bạn.",
                "suggestions": [
                    "Xem định mức kỹ thuật cho mã SC_NO",
                    "So sánh định mức hoặc báo cáo demand",
                    "Xem dữ liệu submat transaction summary"
                ]
            }
        
        # Validate task conditions
        validation = self.task_manager.validate_task(task_name, query, context)
        
        if not validation["valid"]:
            return {
                "type": "missing_conditions",
                "task_name": task_name,
                "task_description": validation["task"].description,
                "message": f"Để thực hiện '{validation['task'].description}', bạn cần bổ sung:",
                "missing_conditions": [
                    f"• {cond.description}" for cond in validation["missing_conditions"]
                ],
                "example": self._get_task_example(task_name)
            }
        
        # Execute task
        try:
            result = await self.task_manager.execute_task(
                task_name, 
                validation["satisfied_conditions"], 
                self,
                context
            )
            
            return {
                "type": "success",
                "task_name": task_name,
                "task_description": validation["task"].description,
                "data": result
            }
            
        except Exception as e:
            return {
                "type": "error",
                "message": f"Lỗi khi thực thi tác vụ: {str(e)}"
            }
    
    def _get_task_example(self, task_name: str) -> str:
        """Trả về ví dụ cho task"""
        examples = {
            "dm_technical": "VD: 'Xem DM Technical cho S24M12345'",
            "dm_actual": "VD: 'Xem DM Actual cho S24M12345'", 
            "compare": "VD: 'So sánh DM cho S24M12345'",
            "process_wip": "VD: 'Xem tiến độ cho S24M12345'",
            "insert_trims": "VD: 'Cập nhật trims list' (cần tải file trước)"
        }
        return examples.get(task_name, "")
    
    async def warm_up_connections(self):
        """Warm up database connections"""
        try:
            await asyncio.sleep(0.1)  # Đợi app khởi động xong
            # Warm up database connection
            await asyncio.to_thread(self.supabase.get_data, "list_go LIMIT 1", ' "SC_NO" ')
        except Exception as e:
            print(f"Warm up error: {e}")
            
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
    