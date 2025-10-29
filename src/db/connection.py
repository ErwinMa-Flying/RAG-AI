"""
数据库连接管理模块

职责：
- 读取数据库配置（从 .env）
- 创建和管理数据库连接
- 提供连接获取接口

使用方式：
    from db.connection import get_connection
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM knowledge_base")
    # ...
    cursor.close()
    conn.close()
"""

import os
import logging
from pathlib import Path
from typing import Optional
import pymysql
from pymysql import Error
from dotenv import load_dotenv

# 配置日志
logger = logging.getLogger(__name__)

# 加载 .env 文件（从项目根目录）
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
    logger.info(f"Loaded environment variables from {ENV_FILE}")
else:
    logger.warning(f".env file not found at {ENV_FILE}, using system environment variables")


class DatabaseConfig:
    """数据库配置类"""
    
    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = int(os.getenv("DB_PORT", "3306"))
        self.user = os.getenv("DB_USER", "root")
        self.password = os.getenv("DB_PASSWORD", "")
        self.database = os.getenv("DB_NAME", "rag_database")
    
    def to_dict(self) -> dict:
        """转换为 pymysql 需要的字典格式"""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": "utf8mb4",
            "autocommit": False,  # 手动控制事务
        }
    
    def validate(self):
        """验证必需的配置项"""
        if not self.password:
            raise ValueError("DB_PASSWORD is not set in .env file")
        if not self.database:
            raise ValueError("DB_NAME is not set in .env file")
    
    def __repr__(self):
        # 隐藏密码
        return (f"DatabaseConfig(host={self.host}, port={self.port}, "
                f"user={self.user}, database={self.database})")


def get_connection():
    """
    获取数据库连接
    
    Returns:
        pymysql.connections.Connection: 数据库连接对象
    
    Raises:
        Error: 连接失败时抛出异常
    
    Example:
        >>> conn = get_connection()
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT 1")
        >>> result = cursor.fetchone()
        >>> print(result)
        (1,)
        >>> cursor.close()
        >>> conn.close()
    """
    config = DatabaseConfig()
    
    # 验证配置
    try:
        config.validate()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        raise
    
    # 尝试连接
    try:
        connection = pymysql.connect(**config.to_dict())
        
        logger.info(f"Successfully connected to MySQL Server")
        logger.debug(f"Connection config: {config}")
        return connection
            
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        logger.error(f"Config used: {config}")
        raise


def test_connection() -> bool:
    """
    测试数据库连接是否正常
    
    Returns:
        bool: 连接成功返回 True，失败返回 False
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # 测试1：执行简单查询
        cursor.execute("SELECT 1 AS test")
        result = cursor.fetchone()
        logger.info(f"✅ Basic query result: {result}")
        
        # 测试2：检查数据库和表是否存在
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]
        logger.info(f"✅ Existing tables: {table_names}")
        
        # 测试3：检查必需的表
        required_tables = ['knowledge_base', 'knowledge_file', 'file_doc']
        missing_tables = [t for t in required_tables if t not in table_names]
        
        if missing_tables:
            logger.warning(f"⚠️  Missing tables: {missing_tables}")
            logger.warning("Please create these tables before running the application")
        else:
            logger.info(f"✅ All required tables exist")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Error as e:
        logger.error(f"❌ Database connection test failed: {e}")
        return False


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("="*60)
    print("Testing database connection...")
    print("="*60)
    print()
    
    if test_connection():
        print()
        print("="*60)
        print("✅ Connection successful!")
        print("="*60)
        print()
        print("Next steps:")
        print("1. If tables are missing, run the SQL scripts to create them")
        print("2. Proceed to implement Repository layer")
    else:
        print()
        print("="*60)
        print("❌ Connection failed!")
        print("="*60)
        print()
        print("Please check:")
        print("1. MySQL server is running")
        print("2. .env file exists with correct credentials:")
        print(f"   Location: {ENV_FILE}")
        print("3. Database 'rag_database' exists")
        print("4. User has proper permissions")