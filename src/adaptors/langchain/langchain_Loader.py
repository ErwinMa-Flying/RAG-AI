"""
Langchain Loader 适配器

职责：
- 封装 Langchain 的文档加载器
- 继承 BaseLoader，提供统一接口
- 处理不同文件类型

支持的文件类型：
- PDF: PyPDFLoader
- Word: Docx2txtLoader
- Text: TextLoader
- Markdown: UnstructuredMarkdownLoader
"""

import logging
from pathlib import Path
from typing import Iterable, Sequence, Dict, Any, Optional

from langchain_community.document_loaders import (
    PyPDFLoader,
    Docx2txtLoader,
    TextLoader,
    UnstructuredMarkdownLoader,
)

from core.types import RawDoc
from core.base_loader import BaseLoader

logger = logging.getLogger(__name__)


class LangchainLoaderAdapter(BaseLoader):
    """
    Langchain 文档加载器适配器
    
    继承 BaseLoader，提供统一的文档加载接口
    支持 PDF、Word、Text、Markdown 等格式
    """
    
    # ========================================
    # ① 覆写：声明支持的文件扩展名
    # ========================================
    @classmethod
    def allowed_exts(cls) -> tuple[str, ...]:
        """声明当前 Loader 支持的文件扩展名"""
        return (".pdf", ".docx", ".txt", ".md")
    
    def __init__(self):
        """初始化 Loader 适配器"""
        super().__init__()
        
        # 定义扩展名到 Loader 类的映射
        self._loader_map = {
            '.pdf': PyPDFLoader,
            '.docx': Docx2txtLoader,
            '.txt': TextLoader,
            '.md': UnstructuredMarkdownLoader,
        }
        
        logger.info(f"LangchainLoaderAdapter initialized with {len(self._loader_map)} supported formats")
    
    def _get_loader_config(self, ext: str) -> Dict[str, Any]:
        """
        根据文件扩展名获取 Loader 配置
        
        Args:
            ext: 文件扩展名（如 '.txt', '.pdf'）
        
        Returns:
            Dict[str, Any]: Loader 配置字典
        
        规则：
        - .txt: 需要指定编码，使用 autodetect_encoding
        - .pdf: 不需要额外配置
        - .docx: 不需要额外配置
        - .md: 不需要额外配置
        """
        if ext == '.txt':
            # TextLoader 需要编码配置
            return {
                'encoding': 'utf-8',
                'autodetect_encoding': True  # 自动检测编码
            }
        elif ext == '.pdf':
            # PyPDFLoader 不需要编码参数
            return {}
        elif ext == '.docx':
            # Docx2txtLoader 不需要编码参数
            return {}
        elif ext == '.md':
            # UnstructuredMarkdownLoader 不需要编码参数
            return {}
        else:
            return {}
    
    # ========================================
    # ② 实现抽象方法：load()
    # ========================================
    def load(
        self,
        paths: Sequence[Path],
        options: Optional[Dict[str, Any]] = None,
    ) -> Iterable[RawDoc]:
        """
        加载文件并返回 RawDoc 序列（生成器，内存友好）
        
        实现 BaseLoader 的抽象方法
        
        Args:
            paths: 文件路径列表
            options: 可选配置参数（保留，暂未使用）
        
        Yields:
            RawDoc: 文档对象
        """
        logger.info(f"Loading {len(paths)} file(s)")
        
        for file_path in paths:
            # 验证文件存在性
            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                continue
            
            # 获取文件扩展名
            ext = file_path.suffix.lower()
            
            # 选择对应的 Loader 类
            loader_class = self._loader_map.get(ext)
            if not loader_class:
                logger.warning(f"No loader found for extension: {ext}")
                continue
            
            # 实例化 Langchain Loader
            try:
                # ← 关键：根据文件类型获取配置
                loader_config = self._get_loader_config(ext)
                
                # 创建 Loader 实例
                if loader_config:
                    logger.debug(f"Creating {loader_class.__name__} with config: {loader_config}")
                    loader = loader_class(str(file_path), **loader_config)
                else:
                    logger.debug(f"Creating {loader_class.__name__} without config")
                    loader = loader_class(str(file_path))
                
                logger.debug(f"Loading {file_path.name} with {loader_class.__name__}")
                
                # 加载文档
                langchain_docs = loader.load()
                
                # 检查是否为空
                if not langchain_docs:
                    logger.warning(f"No documents loaded from {file_path.name} (file might be empty)")
                    continue
                
                # 转换为 RawDoc 对象并 yield
                page_count = 0
                for doc in langchain_docs:
                    # 检查内容是否为空
                    if not doc.page_content or not doc.page_content.strip():
                        logger.debug(f"Skipping empty document from {file_path.name}")
                        continue
                    
                    raw_doc = RawDoc(
                        text=doc.page_content,
                        metadata=doc.metadata
                    )
                    page_count += 1
                    yield raw_doc
                
                if page_count > 0:
                    logger.info(f"Loaded {page_count} page(s) from {file_path.name}")
                else:
                    logger.warning(f"No valid documents found in {file_path.name}")
                
            except Exception as e:
                logger.error(f"Failed to load {file_path.name}: {e}")
                logger.debug(f"Traceback:", exc_info=True)
                continue
    
    # ========================================
    # ③ 新增：获取解析器名称（可选，用于数据库记录）
    # ========================================
    def get_parser_name(self, file_path: Path) -> str:
        """
        获取文件对应的解析器名称
        
        用于数据库的 parser_profile 字段
        
        Args:
            file_path: 文件路径
        
        Returns:
            str: 解析器名称（如 'pdf', 'docx', 'txt'）
        """
        ext = file_path.suffix.lower()
        
        parser_map = {
            '.pdf': 'pdf_parser',
            '.docx': 'docx_parser',
            '.txt': 'txt_parser',
            '.md': 'markdown_parser',
        }
        
        return parser_map.get(ext, 'unknown')