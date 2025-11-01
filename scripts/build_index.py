"""
批量构建索引

功能：
- 扫描 data/raw 目录下的所有文件
- 调用 IndexingService 逐个处理
- 打印处理结果（包括 RawDoc 内容预览）
- 存储到数据库

使用方式：
    python scripts/build_index.py
"""

import sys
from pathlib import Path

# 添加 src 到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import logging
from services.indexing_service import IndexingService
from adaptors.langchain.langchain_Loader import LangchainLoaderAdapter

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def scan_files(directory: Path) -> list[Path]:
    """
    扫描目录下的所有支持的文件
    
    Args:
        directory: 目标目录
    
    Returns:
        List[Path]: 文件路径列表
    """
    # 支持的文件扩展名
    supported_extensions = {'.pdf', '.docx', '.txt', '.md'}
    
    files = []
    
    if not directory.exists():
        logger.warning(f"Directory does not exist: {directory}")
        return files
    
    # 递归扫描所有文件
    for file_path in directory.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in supported_extensions:
            files.append(file_path)
    
    return files


def print_raw_doc(raw_doc, index: int, max_preview_length: int = 200):
    """
    打印单个 RawDoc 的内容
    
    Args:
        raw_doc: RawDoc 对象
        index: 文档序号
        max_preview_length: 最大预览长度
    """
    print(f"   📄 Document {index}:")
    print(f"      Text length: {len(raw_doc.text)} characters")
    
    # 打印文本预览
    preview = raw_doc.text[:max_preview_length]
    if len(raw_doc.text) > max_preview_length:
        preview += "..."
    print(f"      Preview: {preview}")
    
    # 打印 metadata
    if raw_doc.metadata:
        print(f"      Metadata: {raw_doc.metadata}")
    print()


def build_index_for_kb(kb_id: str, data_dir: Path):
    """
    为指定知识库构建索引
    
    Args:
        kb_id: 知识库 ID
        data_dir: 数据目录（如 data/raw）
    """
    print("="*60)
    print(f"Building index for Knowledge Base: {kb_id}")
    print("="*60)
    print()
    
    # 1. 扫描文件
    print(f"Scanning files in: {data_dir}")
    files = scan_files(data_dir)
    print(f"Found {len(files)} file(s)")
    print()
    
    if not files:
        print("⚠️  No files found. Exiting.")
        return
    
    # 打印文件列表
    print("Files to be indexed:")
    for i, file_path in enumerate(files, 1):
        # 显示相对路径（相对于 data_dir）
        rel_path = file_path.relative_to(data_dir)
        print(f"  {i}. {rel_path} ({file_path.stat().st_size} bytes)")
    print()
    
    # 2. 创建 IndexingService 和 Loader
    service = IndexingService(kb_id=kb_id)
    loader = LangchainLoaderAdapter()  # ← 新增：创建 Loader 用于预览
    
    # 3. 逐个处理文件
    success_count = 0
    failed_count = 0
    results = []
    
    print("="*60)
    print("Processing files...")
    print("="*60)
    print()
    
    for i, file_path in enumerate(files, 1):
        rel_path = file_path.relative_to(data_dir)
        print(f"[{i}/{len(files)}] Processing: {rel_path}")
        print("-" * 60)
        
        try:
            # ========================================
            # ① 先加载文档（预览用）
            # ========================================
            print(f"📖 Loading documents from {file_path.name}...")
            raw_docs_generator = loader.load(paths=[file_path], options=None)
            raw_docs = list(raw_docs_generator)  # 转换为列表
            
            print(f"   ✅ Loaded {len(raw_docs)} document(s)")
            print()
            
            # ========================================
            # ② 打印每个 RawDoc 的内容
            # ========================================
            if raw_docs:
                print(f"📋 Document contents:")
                print()
                for doc_index, raw_doc in enumerate(raw_docs, 1):
                    print_raw_doc(raw_doc, doc_index, max_preview_length=200)
            else:
                print(f"   ⚠️  No documents found in {file_path.name}")
                print()
            
            # ========================================
            # ③ 索引文件（存入数据库）
            # ========================================
            print(f"💾 Indexing to database...")
            kf_id = service.index_file(file_path)
            
            # 记录结果
            result = {
                'file_name': file_path.name,
                'file_path': str(file_path),
                'rel_path': str(rel_path),
                'kf_id': kf_id,
                'doc_count': len(raw_docs),
                'status': 'success'
            }
            results.append(result)
            success_count += 1
            
            print(f"✅ Success!")
            print(f"   kf_id: {kf_id}")
            print(f"   Status: parsed")
            print(f"   Documents: {len(raw_docs)}")
            
        except Exception as e:
            # 记录失败
            result = {
                'file_name': file_path.name,
                'file_path': str(file_path),
                'rel_path': str(rel_path),
                'kf_id': None,
                'doc_count': 0,
                'status': 'failed',
                'error': str(e)
            }
            results.append(result)
            failed_count += 1
            
            print(f"❌ Failed!")
            print(f"   Error: {e}")
        
        print()
    
    # 4. 打印汇总
    print("="*60)
    print("Summary")
    print("="*60)
    print()
    print(f"Total files:     {len(files)}")
    print(f"✅ Success:      {success_count}")
    print(f"❌ Failed:       {failed_count}")
    print()
    
    # 打印文档统计
    total_docs = sum(r.get('doc_count', 0) for r in results if r['status'] == 'success')
    print(f"Total documents loaded: {total_docs}")
    print()
    
    # 5. 打印详细结果
    if success_count > 0:
        print("Successfully indexed files:")
        for result in results:
            if result['status'] == 'success':
                print(f"  ✅ {result['rel_path']}")
                print(f"     kf_id: {result['kf_id']}")
                print(f"     Documents: {result['doc_count']}")
    print()
    
    if failed_count > 0:
        print("Failed files:")
        for result in results:
            if result['status'] == 'failed':
                print(f"  ❌ {result['rel_path']}")
                print(f"     Error: {result['error']}")
    print()
    
    # 6. 数据库查询提示
    print("="*60)
    print("Database Queries")
    print("="*60)
    print()
    print("Check indexed files:")
    print(f"  SELECT * FROM knowledge_file WHERE kb_id = '{kb_id}';")
    print()
    print("Check specific file:")
    if success_count > 0:
        first_success = next(r for r in results if r['status'] == 'success')
        print(f"  SELECT * FROM knowledge_file WHERE kf_id = '{first_success['kf_id']}';")
    print()


def main():
    """
    主函数
    """
    # 配置参数
    TEST_KB_ID = '11111111-1111-1111-1111-111111111111'
    DATA_DIR = PROJECT_ROOT / "data" / "raw"
    
    print()
    print("="*60)
    print("RAG System - Index Builder")
    print("="*60)
    print()
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Data Directory: {DATA_DIR}")
    print(f"Knowledge Base ID: {TEST_KB_ID}")
    print()
    
    # 检查目录是否存在
    if not DATA_DIR.exists():
        print(f"❌ Error: Directory does not exist: {DATA_DIR}")
        print()
        print("Please create the directory and add some files:")
        print(f"  mkdir -p {DATA_DIR}")
        print(f"  # Then add some PDF/DOCX/TXT files to {DATA_DIR}")
        print()
        return 1
    
    # 构建索引
    try:
        build_index_for_kb(TEST_KB_ID, DATA_DIR)
        print("="*60)
        print("✅ Index building completed!")
        print("="*60)
        print()
        return 0
        
    except Exception as e:
        print("="*60)
        print(f"❌ Index building failed: {e}")
        print("="*60)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())