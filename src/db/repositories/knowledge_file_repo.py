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
        file_mtime: Optional[datetime] = None,
        checksum: Optional[str] = None,           # ← 新增：Loader 阶段可以计算
        parser_profile: Optional[str] = None,     # ← 新增：Loader 阶段知道用了哪个解析器
        version: int = 1,  # ← 新增：默认版本号为 1
        custom_docs: int = 0,  # ← 新增：默认无自定义（0=否，1=是）
    ) -> str:
        """
        插入文件记录（Loader 阶段调用）
        
        必填参数：
            kb_id: 知识库 ID
            source_uri: 文件路径/URL
            file_name: 文件名
            file_ext: 扩展名（如 '.pdf'）
            file_size: 文件大小（字节）
        
        可选参数：
            source_type: 来源类型（'path', 'upload', 'url' 等）
            file_mtime: 文件修改时间
            checksum: 文件 MD5/SHA256（Loader 可以顺便计算）
            parser_profile: 解析器配置（Loader 知道用了哪个解析器）
            version: 版本号（默认 1，表示第一版）
            custom_docs: 是否包含自定义文档（0=否，1=是，默认 0）
        
        Returns:
            str: 生成的 kf_id
        
        Example:
            >>> # 最简单的调用（使用默认 version=1, custom_docs=0）
            >>> kf_id = repo.insert(
            ...     kb_id='kb-001',
            ...     source_type='path',
            ...     source_uri='data/doc.pdf',
            ...     file_name='doc.pdf',
            ...     file_ext='.pdf',
            ...     file_size=12345
            ... )
            >>> 
            >>> # 上传新版本
            >>> kf_id_v2 = repo.insert(
            ...     kb_id='kb-001',
            ...     source_uri='data/doc_v2.pdf',
            ...     file_name='doc.pdf',
            ...     file_ext='.pdf',
            ...     file_size=23456,
            ...     version=2,  # ← 第2版
            ...     checksum='new_checksum...'
            ... )
            >>> 
            >>> # 包含自定义内容的文档
            >>> kf_id_custom = repo.insert(
            ...     kb_id='kb-001',
            ...     source_uri='data/custom_doc.pdf',
            ...     file_name='custom_doc.pdf',
            ...     file_ext='.pdf',
            ...     file_size=45678,
            ...     custom_docs=1  # ← 标记为自定义文档
            ... )
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
            # 动态构建 SQL（根据是否提供可选字段）
            fields = ['kf_id', 'kb_id', 'source_uri', 'file_name', 'file_ext', 
                      'file_size', 'file_mtime', 'status', 'create_time',
                      'version', 'custom_docs']  # ← 添加 version 和 custom_docs
            values = [kf_id, kb_id, source_uri, file_name, file_ext, 
                      file_size, file_mtime, 'parsed', create_time,
                      version, custom_docs]  # ← 添加默认值
            
            # 添加可选字段
            if source_type is not None:
                fields.append('source_type')
                values.append(source_type)
            
            if checksum is not None:
                fields.append('checksum')
                values.append(checksum)
            
            if parser_profile is not None:
                fields.append('parser_profile')
                values.append(parser_profile)
            
            # 构建 SQL
            placeholders = ', '.join(['%s'] * len(values))
            sql = f"""
                INSERT INTO knowledge_file ({', '.join(fields)})
                VALUES ({placeholders})
            """
            
            cursor.execute(sql, values)
            conn.commit()
            
            logger.info(f"Inserted file record: {kf_id} ({file_name}, version={version})")
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
        chunk_profile: Optional[str] = None,
        vector_count: Optional[int] = None,
        embed_model: Optional[str] = None,
    ) -> None:
        """
        更新文件状态（根据不同阶段更新相应字段）
        
        Args:
            kf_id: 文件 ID
            status: 新状态
                - 'pending': 待处理（初始状态）
                - 'parsed': 已解析（Loader 完成）
                - 'chunked': 已分块（Splitter 完成）
                - 'embedded': 已向量化（Embedding 完成）
                - 'failed': 失败
            chunk_count: chunk 数量（status='chunked' 时提供）
            chunk_profile: 分块配置（status='chunked' 时提供，如 'recursive_500_50'）
            vector_count: 向量数量（status='embedded' 时提供）
            embed_model: 向量模型（status='embedded' 时提供，如 'text-embedding-3-small'）
        
        Note:
            - updated_at 字段会自动更新（TIMESTAMP 类型）
            - 不同状态需要提供不同的参数
        
        Example:
            >>> repo = KnowledgeFileRepository()
            >>> 
            >>> # Loader 完成后
            >>> repo.update_status('uuid-123', 'parsed')
            >>> 
            >>> # Splitter 完成后
            >>> repo.update_status(
            ...     'uuid-123', 
            ...     'chunked',
            ...     chunk_count=10,
            ...     chunk_profile='recursive_500_50'
            ... )
            >>> 
            >>> # Embedding 完成后
            >>> repo.update_status(
            ...     'uuid-123',
            ...     'embedded',
            ...     vector_count=10,
            ...     embed_model='text-embedding-3-small'
            ... )
            >>> 
            >>> # 失败时
            >>> repo.update_status('uuid-456', 'failed')
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        try:
            # 根据不同状态更新不同字段
            if status == 'chunked':
                # Splitter 完成：更新 chunk_count 和 chunk_profile
                sql = """
                    UPDATE knowledge_file 
                    SET status = %s, chunk_count = %s, chunk_profile = %s
                    WHERE kf_id = %s
                """
                cursor.execute(sql, (
                    status,
                    chunk_count or 0,
                    chunk_profile or 'default',
                    kf_id
                ))
                logger.info(f"Updated {kf_id}: status='{status}', chunk_count={chunk_count}, chunk_profile='{chunk_profile}'")
            
            elif status == 'embedded':
                # Embedding 完成：更新 vector_count 和 embed_model
                sql = """
                    UPDATE knowledge_file 
                    SET status = %s, vector_count = %s, embed_model = %s
                    WHERE kf_id = %s
                """
                cursor.execute(sql, (
                    status,
                    vector_count or 0,
                    embed_model or 'unknown',
                    kf_id
                ))
                logger.info(f"Updated {kf_id}: status='{status}', vector_count={vector_count}, embed_model='{embed_model}'")
            
            else:
                # 其他状态（pending/parsed/failed）：只更新 status
                sql = """
                    UPDATE knowledge_file 
                    SET status = %s
                    WHERE kf_id = %s
                """
                cursor.execute(sql, (status, kf_id))
                logger.info(f"Updated {kf_id}: status='{status}'")
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update status for {kf_id}: {e}")
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