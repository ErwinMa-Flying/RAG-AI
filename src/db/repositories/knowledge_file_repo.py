"""
KnowledgeFile 表的数据访问层

职责：
- 封装 knowledge_file 表的所有数据库操作
- 插入文件记录
- 更新文件状态
- 查询文件信息

不关心：
- 文件如何读取（Loader 的职责）
- 业务逻辑（Service 的职责）
"""

import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
import pymysql.cursors

from db.connection import get_connection

logger = logging.getLogger(__name__)


class KnowledgeFileRepository:
    """KnowledgeFile 表的 Repository"""
    
    def __init__(self):
        """初始化 Repository"""
        pass
    
    def insert(
        self,
        kb_id: str,
        source_uri: str,
        file_name: str,
        file_ext: str,
        file_size: int,
        source_type: Optional[str] = None,  # 'upload', 'path', 'url', 'api', 'crawl', 'manual'
        file_mtime: Optional[datetime] = None
    ) -> str:
        """
        插入文件记录
        
        Args:
            kb_id: 知识库 ID
            source_uri: 源 URI（文件路径）
            file_name: 文件名（如 document.pdf）
            file_ext: 文件扩展名（如 .pdf）
            file_size: 文件大小（字节）
            source_type: 来源类型（可选），有效值：
                - 'upload': 上传文件
                - 'path': 本地路径
                - 'url': URL 链接
                - 'api': API 接口
                - 'crawl': 爬虫抓取
                - 'manual': 手动输入
            file_mtime: 文件修改时间（可选）
        
        Returns:
            str: 生成的 kf_id
        
        Raises:
            Exception: 插入失败时抛出异常
        
        Example:
            >>> repo = KnowledgeFileRepository()
            >>> file_id = repo.insert(
            ...     kb_id='kb-001',
            ...     source_type='path',
            ...     source_uri='data/raw/kb-001/document.pdf',
            ...     file_name='document.pdf',
            ...     file_ext='.pdf',
            ...     file_size=12345
            ... )
            >>> print(file_id)
            'uuid-123-456-789'
        """
        # 生成 kf_id（36位 UUID）
        kf_id = str(uuid.uuid4())
        create_time = datetime.now()
        
        # 如果没有提供 file_mtime，使用当前时间
        if file_mtime is None:
            file_mtime = create_time
        
        conn = get_connection()
        cursor = conn.cursor()
        
        try:
            # 注意：不需要插入 updated_at，因为它是 TIMESTAMP 类型，会自动设置
            sql = """
                INSERT INTO knowledge_file 
                (kf_id, kb_id, source_type, source_uri, file_name, file_ext, 
                 file_size, file_mtime, status, create_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(sql, (
                kf_id,
                kb_id,
                source_type,  # ENUM: 'local' 或 'upload'
                source_uri,
                file_name,
                file_ext,
                file_size,
                file_mtime,
                'pending',  # ENUM 初始状态
                create_time
            ))
            
            conn.commit()
            logger.info(f"Inserted file record: {kf_id} ({file_name})")
            
            return kf_id
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to insert file record: {e}")
            raise
        
        finally:
            cursor.close()
            conn.close()
    
    def update_status(
        self,
        kf_id: str,
        status: str,  # ENUM: 'pending', 'parsed', 'chunked', 'embedded', 'failed'
        chunk_count: Optional[int] = None,
        vector_count: Optional[int] = None
    ) -> None:
        """
        更新文件状态
        
        Args:
            kf_id: 文件 ID
            status: 新状态
                - 'pending': 待处理
                - 'parsed': 已解析（Loader 完成）
                - 'chunked': 已分块（Splitter 完成）
                - 'embedded': 已向量化（Embedding 完成）
                - 'failed': 失败
            chunk_count: chunk 数量（status='chunked' 或 'embedded' 时提供）
            vector_count: 向量数量（status='embedded' 时提供）
        
        Raises:
            Exception: 更新失败时抛出异常
        
        Example:
            >>> repo = KnowledgeFileRepository()
            >>> repo.update_status('uuid-123', 'parsed')
            >>> repo.update_status('uuid-123', 'chunked', chunk_count=10)
            >>> repo.update_status('uuid-123', 'embedded', chunk_count=10, vector_count=10)
            >>> repo.update_status('uuid-456', 'failed')
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        try:
            # 如果状态是 embedded（最终状态），更新统计信息
            if status == 'embedded':
                sql = """
                    UPDATE knowledge_file 
                    SET status = %s, chunk_count = %s, vector_count = %s
                    WHERE kf_id = %s
                """
                cursor.execute(sql, (status, chunk_count or 0, vector_count or 0, kf_id))
            
            # 如果状态是 chunked，只更新 chunk_count
            elif status == 'chunked':
                sql = """
                    UPDATE knowledge_file 
                    SET status = %s, chunk_count = %s
                    WHERE kf_id = %s
                """
                cursor.execute(sql, (status, chunk_count or 0, kf_id))
            
            # 其他状态（pending/parsed/failed）
            else:
                sql = """
                    UPDATE knowledge_file 
                    SET status = %s
                    WHERE kf_id = %s
                """
                cursor.execute(sql, (status, kf_id))
            
            conn.commit()
            logger.info(f"Updated file {kf_id} status to '{status}'")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update file status: {e}")
            raise
        
        finally:
            cursor.close()
            conn.close()
    
    def get_by_id(self, kf_id: str) -> Optional[Dict[str, Any]]:
        """
        根据 kf_id 查询文件信息
        
        Args:
            kf_id: 文件 ID
        
        Returns:
            Dict: 文件信息字典，如果不存在返回 None
        
        Example:
            >>> repo = KnowledgeFileRepository()
            >>> file_info = repo.get_by_id('uuid-123')
            >>> print(file_info)
            {
                'kf_id': 'uuid-123',
                'kb_id': 'kb-001',
                'file_name': 'document.pdf',
                'file_size': 12345,
                'status': 'completed',
                ...
            }
        """
        conn = get_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # 返回字典格式
        
        try:
            sql = """
                SELECT kf_id, kb_id, source_type, source_uri, file_name, file_ext,
                       file_size, file_mtime, checksum, version, parser_profile, chunk_profile,
                       status, embed_model, custom_docs, chunk_count, vector_count,
                       create_time, updated_at
                FROM knowledge_file
                WHERE kf_id = %s
            """
            
            cursor.execute(sql, (kf_id,))
            result = cursor.fetchone()
            
            if result:
                logger.debug(f"Found file: {kf_id}")
            else:
                logger.warning(f"File not found: {kf_id}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to query file: {e}")
            raise
        
        finally:
            cursor.close()
            conn.close()
    
    def get_by_kb_id(self, kb_id: str) -> list:
        """
        查询知识库的所有文件
        
        Args:
            kb_id: 知识库 ID
        
        Returns:
            list: 文件信息列表
        
        Example:
            >>> repo = KnowledgeFileRepository()
            >>> files = repo.get_by_kb_id('kb-001')
            >>> for file in files:
            ...     print(file['file_name'], file['status'])
        """
        conn = get_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        try:
            sql = """
                SELECT kf_id, kb_id, source_type, source_uri, file_name, file_ext,
                       file_size, status, chunk_count, vector_count, create_time, updated_at
                FROM knowledge_file
                WHERE kb_id = %s
                ORDER BY create_time DESC
            """
            
            cursor.execute(sql, (kb_id,))
            results = cursor.fetchall()
            
            logger.info(f"Found {len(results)} files for kb_id={kb_id}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to query files: {e}")
            raise
        
        finally:
            cursor.close()
            conn.close()