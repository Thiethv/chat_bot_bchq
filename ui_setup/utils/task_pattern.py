import asyncio
import re
from typing import Optional

import numpy as np
from ui_setup.utils.data_processor import DataProcessor


class TaskPattern:
    def __init__(self):
        self.task_patterns = self._define_task_patterns()
        self.normalize_text = DataProcessor().normalize_text
        self._embedding_model = None
        self.task_embeddings = None
    
    # KẾT HỢP GIỮ EMBEDINGS MODEL VÀ TASK PATTERNS
    async def _get_embedding_model(self):
        """Lazy loading cho embedding model"""
        
        if self._embedding_model is None:
            from sentence_transformers import SentenceTransformer
            self._embedding_model = await asyncio.to_thread(
                SentenceTransformer, "thenlper/gte-base"
            )
        return self._embedding_model
    
    async def _generate_task_embeddings(self):
        model = await self._get_embedding_model()
        embeddings = {}
        for task, config in self.task_patterns.items():
            # Kết hợp mô tả và keywords
            text = f"{config['description']} {' '.join(config['primary_keywords'])}"
            embeddings[task] = model.encode(text)
        
        self.task_embeddings = embeddings
        # return embeddings
    
    # Advanced method
    def _define_task_patterns(self):
        """Định nghĩa patterns cho từng task với độ ưu tiên"""
        """Sử dụng REGEX patterns và keywords thay vì embedding"""
        return {
            "compare": {
                "description": "So sánh định mức, báo cáo so sánh GO",
                "primary_keywords": ["so sánh", "compare", "comparison", "go comparison", "demand"],
                "secondary_keywords": ["báo cáo so sánh", "compare report", "báo cáo so sánh go", "go comparison", "báo cáo demand"],
                "must_have_patterns": [
                    r"so\s*sánh(?P<type>technical|actual|định mức)",
                    r"compare.*?(technical|actual|dm)",
                    r"báo cáo.*?so sánh",
                    r"report.*?go",
                    r"go.*?comparison"
                    r"báo cáo.*?demand"
                ],
                "priority": 10
            },
            
            "dm_technical": {
                "description": "Báo cáo định mức kỹ thuật, dm technical",
                "primary_keywords": ["dm technical", "định mức kỹ thuật", "technical"],
                "secondary_keywords": ["báo cáo technical", "report technical"],
                "must_have_patterns": [
                    r"(dm|định mức).*?technical",
                    r"báo cáo.*?(technical|kỹ thuật)",
                    r"report.*?technical"
                ],
                "exclude_patterns": [r"so sánh", r"compare"],  # Loại trừ nếu có từ so sánh
                "priority": 8
            },
            
            "dm_actual": {
                "description": "Báo cáo định mức thực tế, dm actual",
                "primary_keywords": ["dm actual", "định mức thực tế", "actual"],
                "secondary_keywords": ["báo cáo actual", "report actual"],
                "must_have_patterns": [
                    r"(dm|định mức).*?actual",
                    r"báo cáo.*?(actual|thực tế)",
                    r"report.*?actual"
                ],
                "exclude_patterns": [r"so sánh", r"compare"],
                "priority": 8
            },
            
            "process_wip": {
                "description": "Số lượng wip, process wip",
                "primary_keywords": ["process wip", "tiến độ", "wip"],
                "secondary_keywords": ["báo cáo tiến độ", "progress report"],
                "must_have_patterns": [
                    r"process.*?wip",
                    r"báo cáo.*?tiến độ",
                    r"tiến độ.*?sản xuất"
                ],
                "priority": 6
            },
            "cutting_forecast": {
                "description": "Số lượng sản xuất, cutting forecast",
                "primary_keywords": ["cutting forecast", "cutting", "forecast"],
                "secondary_keywords": ["báo cáo cutting", "cutting report"],
                "must_have_patterns": [
                    r"cutting.*?forecast",
                    r"báo cáo.*?cutting",
                    r"cutting.*?sản xuất"
                ],
                "priority": 6
            },
            "fabric_trans": {
                "description": "Tổng hợp giao dịch fabric, fabric trans summary",
                "primary_keywords": ["fabric trans summary", "fabric", "transaction"],
                "secondary_keywords": ["tổng hợp giao dịch fabric", "fabric inquiry", "fabric summary"],
                "must_have_patterns": [
                    r"fabric.*?trans",
                    r"tổng hợp.*?giao dịch.*?fabric",
                    r"fabric.*?summary",
                    r"fabric.*?inquiry"  
                ],
                "priority": 6
            },
            "submat_trans": {
                "description": "Tổng hợp giao dịch submat, submat trans summary",
                "primary_keywords": ["submat trans summary", "submat", "trans"],
                "secondary_keywords": ["tổng hợp giao dịch submat", "submat inquiry", "submat summary"],
                "must_have_patterns": [
                    r"submat.*?trans",
                    r"tổng hợp.*?giao dịch.*?submat",
                    r"submat.*?summary"    
                ],
                "priority": 6
            },
            "submat_demand": {
                "description": "Yêu cầu nguyên phụ liệu, submat demand",
                "primary_keywords": ["submat demand", "submat", "demand"],
                "secondary_keywords": ["yêu cầu submat", "yêu cầu nguyên phụ liệu", "submat demand list"],
                "must_have_patterns": [
                    r"submat.*?demand",
                    r"yêu cầu.*?(submat|nguyên phụ liệu)",
                    r"submat.*?demand.*?list"    
                ],
                "priority": 6
            },
            "go_quantity": {
                "description": "Yêu cầu số lượng go, quantity",
                "primary_keywords": ["go quantity", "quantity"],
                "secondary_keywords": ["số lượng go"],
                "must_have_patterns": [
                    r"yêu cầu.*?(số lượng|quantity).*?go",
                    r"go.*?quantity"   
                ],
                "priority": 5
            },
            "insert_trims": {
                "description": "Insert trims, cập nhật trims, trims list",
                "primary_keywords": ["insert trims", "cập nhật trims", "trims list"],
                "secondary_keywords": ["cập nhật danh sách phụ liệu", "trims update"],
                "must_have_patterns": [
                    r"insert.*?trims",
                    r"cập nhật.*?trims",
                    r"trims.*?list"
                ],
                "exclude_patterns": [r"báo cáo", r"report"],  # Loại trừ nếu có từ báo cáo
                "priority": 6
            },
            "insert_fabric": {
                "description": "Insert fabric, cập nhật fabric, fabric list",
                "primary_keywords": ["insert fabric", "cập nhật fabric", "fabric list"],
                "secondary_keywords": ["cập nhật danh sách vải", "fabric update"],
                "must_have_patterns": [
                    r"insert.*?fabric",
                    r"cập nhật.*?fabric",
                    r"fabric.*?list"
                ],
                "exclude_patterns": [r"báo cáo", r"report"],  # Loại trừ nếu có từ báo cáo
                "priority": 6
            },
            "insert_range_dm": {
                "description": "Insert range dm, cập nhật range dm, range dm list",
                "primary_keywords": ["insert range dm", "cập nhật range dm", "range dm list"],
                "secondary_keywords": ["cập nhật danh sách dm", "range dm update"],
                "must_have_patterns": [
                    r"insert.*?range.*?dm",
                    r"cập nhật.*?range.*?dm",
                    r"range.*?dm.*?list"
                ],
                "exclude_patterns": [r"báo cáo", r"report"],  # Loại trừ nếu có từ báo cáo
                "priority": 6
            }
        }
    
    async def identify_task(self, query: str) -> Optional[str]:
        if self.task_embeddings is None:
            await self._generate_task_embeddings()
        model = await self._get_embedding_model()

        query_embed = model.encode(query)
        similarities = {}
        
        for task, task_embed in self.task_embeddings.items():
            cos_sim = np.dot(query_embed, task_embed) / (
                np.linalg.norm(query_embed) * np.linalg.norm(task_embed)
            )
            similarities[task] = cos_sim
        
        best_task = max(similarities, key=similarities.get)
        return best_task if similarities[best_task] > 0.7 else None

    '''def identify_task(self, query: str) -> Optional[str]:
        """Improved task identification với scoring system"""
        normalized_query = self.normalize_text(query)
        task_scores = {}
        
        for task_name, config in self.task_patterns.items():
            score = 0
            
            # Check exclude patterns first
            if "exclude_patterns" in config:
                exclude_found = any(
                    re.search(pattern, normalized_query, re.IGNORECASE) 
                    for pattern in config["exclude_patterns"]
                )
                if exclude_found:
                    continue  # Skip this task
            
            # Primary keywords (high score)
            for keyword in config["primary_keywords"]:
                if keyword in normalized_query:
                    score += 10
            
            # Secondary keywords (medium score)  
            for keyword in config["secondary_keywords"]:
                if keyword in normalized_query:
                    score += 5
            
            # Must-have patterns (highest score)
            for pattern in config["must_have_patterns"]:
                if re.search(pattern, normalized_query, re.IGNORECASE):
                    score += 15
            
            # Apply priority weight
            score *= config["priority"] / 10
            
            if score > 0:
                task_scores[task_name] = score
        
        # Return task with highest score
        if task_scores:
            return max(task_scores.items(), key=lambda x: x[1])[0]
        
        return None'''

    def get_task_confidence(self, query: str) -> dict:
        """Trả về confidence score cho tất cả tasks"""
        normalized_query = self.normalize_text(query)
        task_scores = {}
        
        for task_name, config in self.task_patterns.items():
            score = 0
            details = []
            
            # Check exclude patterns
            if "exclude_patterns" in config:
                exclude_found = any(
                    re.search(pattern, normalized_query, re.IGNORECASE) 
                    for pattern in config["exclude_patterns"]
                )
                if exclude_found:
                    task_scores[task_name] = {"score": 0, "reason": "Excluded by patterns"}
                    continue
            
            # Score calculation with details
            for keyword in config["primary_keywords"]:
                if keyword in normalized_query:
                    score += 10
                    details.append(f"Primary keyword: {keyword}")
            
            for keyword in config["secondary_keywords"]:
                if keyword in normalized_query:
                    score += 5
                    details.append(f"Secondary keyword: {keyword}")
            
            for pattern in config["must_have_patterns"]:
                if re.search(pattern, normalized_query, re.IGNORECASE):
                    score += 15
                    details.append(f"Pattern match: {pattern}")
            
            score *= config["priority"] / 10
            task_scores[task_name] = {
                "score": score,
                "details": details,
                "confidence": min(score / 20, 1.0)  # Normalize to 0-1
            }
        
        return task_scores