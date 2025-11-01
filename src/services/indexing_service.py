"""
IndexingService - 文件索引服务

职责：
- 编排单个文件的索引流程
- 协调 Loader、Repository 等组件
- 处理流程中的错误

当前阶段（极简版）：
- ✅ Loader 加载文档
- ✅ 插入文件记录（包含 checksum）
- ✅ 更新状态为 'parsed'
- ⚪ 暂不分块
- ⚪ 暂不向量化

后续扩展：
- 🔜 Splitter 分块
- 🔜 Embedding 向量化
- 🔜 VectorStore 存储
"""

import logging
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional

from db.repositories.knowledge_file_repo import KnowledgeFileRepository
from adaptors.langchain.langchain_Loader import LangchainLoaderAdapter

logger = logging.getLogger(__name__)


class IndexingService:
    """
    文件索引服务
    
    负责单个文件从"原始文件"到"可检索数据"的完整流程
    """
    
    def __init__(
        self,
        kb_id: str,
        file_repo: Optional[KnowledgeFileRepository] = None,
        loader: Optional[LangchainLoaderAdapter] = None
    ):
        """
        初始化索引服务
        
        Args:
            kb_id: 知识库 ID
            file_repo: 文件记录 Repository（可选，默认创建新实例）
            loader: Loader Adapter（可选，默认创建新实例）
        """
        self.kb_id = kb_id
        self.file_repo = file_repo or KnowledgeFileRepository()
        self.loader = loader or LangchainLoaderAdapter()
        
        logger.info(f"IndexingService initialized for kb_id: {kb_id}")
    
    def index_file(self, file_path: Path) -> str:
        """
        索引单个文件（当前阶段：Loader + 插入记录 + 更新为 parsed）
        
        流程：
        1. 验证文件存在
        2. 计算文件 checksum
        3. 调用 Loader 加载文档（生成器）
        4. 插入文件记录（knowledge_file）
        5. 更新状态为 'parsed'
        
        Args:
            file_path: 文件路径
        
        Returns:
            str: 生成的 kf_id
        
        Raises:
            FileNotFoundError: 文件不存在
            Exception: 处理失败
        """
        kf_id = None
        
        try:
            # ========================================
            # 阶段0：验证文件
            # ========================================
            logger.info(f"Starting to index file: {file_path}")
            
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            if not file_path.is_file():
                raise ValueError(f"Path is not a file: {file_path}")
            
            # ========================================
            # 阶段1：计算 checksum
            # ========================================
            logger.info(f"Calculating checksum for: {file_path.name}")
            checksum = self._calculate_checksum(file_path)
            logger.debug(f"Checksum: {checksum}")
            
            # ========================================
            # 阶段2：Loader 加载文档（生成器）
            # ========================================
            logger.info(f"Loading file: {file_path.name}")
            
            # ← 修改：使用新的签名（paths, options）
            raw_docs_generator = self.loader.load(
                paths=[file_path],  # ← Sequence[Path]
                options=None         # ← Optional[Dict[str, Any]]
            )
            
            # 转换为列表（因为后续需要知道数量）
            raw_docs = list(raw_docs_generator)
            logger.info(f"Loaded {len(raw_docs)} document(s)")
            
            if not raw_docs:
                raise ValueError(f"No documents loaded from file: {file_path}")
            
            # 打印第一个文档的前100个字符（验证加载成功）
            if raw_docs:
                first_doc = raw_docs[0]
                preview = first_doc.text[:100] if hasattr(first_doc, 'text') else str(first_doc)[:100]
                logger.debug(f"First doc preview: {preview}...")
            
            # ========================================
            # 阶段3：插入文件记录
            # ========================================
            logger.info(f"Inserting file record: {file_path.name}")
            file_metadata = self._get_file_metadata(file_path)
            
            kf_id = self.file_repo.insert(
                kb_id=self.kb_id,
                source_type='path',
                source_uri=str(file_path),
                file_name=file_metadata['name'],
                file_ext=file_metadata['ext'],
                file_size=file_metadata['size'],
                file_mtime=file_metadata['mtime'],
                checksum=checksum,
                parser_profile=self.loader.get_parser_name(file_path),
                version=1,
                custom_docs=0
            )
            logger.info(f"Inserted file record: {kf_id}")
            
            # ========================================
            # 阶段4：更新状态为 'parsed'
            # ========================================
            self.file_repo.update_status(kf_id, 'parsed')
            logger.info(f"Updated status to 'parsed': {kf_id}")
            
            # ========================================
            # 完成
            # ========================================
            logger.info(f"✅ Successfully indexed file: {kf_id} ({file_path.name})")
            return kf_id
            
        except Exception as e:
            logger.error(f"❌ Failed to index file: {file_path.name}")
            logger.error(f"Error: {e}", exc_info=True)
            
            # 如果已经插入了 kf_id，更新状态为 'failed'
            if kf_id:
                try:
                    self.file_repo.update_status(kf_id, 'failed')
                    logger.info(f"Updated status to 'failed': {kf_id}")
                except Exception as update_error:
                    logger.error(f"Failed to update status to 'failed': {update_error}")
            
            raise
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """
        计算文件的 MD5 checksum
        
        Args:
            file_path: 文件路径
        
        Returns:
            str: MD5 十六进制字符串
        """
        md5_hash = hashlib.md5()
        
        with open(file_path, 'rb') as f:
            # 分块读取，避免大文件内存溢出
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        
        return md5_hash.hexdigest()
    
    def _get_file_metadata(self, file_path: Path) -> dict:
        """
        获取文件元信息
        
        Args:
            file_path: 文件路径
        
        Returns:
            dict: 文件元信息
                - name: 文件名
                - ext: 扩展名
                - size: 文件大小（字节）
                - mtime: 修改时间
        """
        stat = file_path.stat()
        
        return {
            'name': file_path.name,
            'ext': file_path.suffix,
            'size': stat.st_size,
            'mtime': datetime.fromtimestamp(stat.st_mtime)
        }