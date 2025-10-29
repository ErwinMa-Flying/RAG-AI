"""
Repository 层（数据访问层）

封装所有数据库表的增删改查操作
"""

from .knowledge_file_repo import KnowledgeFileRepository

__all__ = [
    "KnowledgeFileRepository",
]