"""
测试 KnowledgeFileRepository 的独立脚本
"""
import sys
from pathlib import Path

# 添加 src 到路径
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import logging
from db.repositories.knowledge_file_repo import KnowledgeFileRepository

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def main():
    print("="*60)
    print("Testing KnowledgeFileRepository...")
    print("="*60)
    print()
    
    # 使用已存在的知识库 ID
    TEST_KB_ID = '11111111-1111-1111-1111-111111111111'
    
    repo = KnowledgeFileRepository()
    
    try:
        # ========================================
        # 测试1：插入文件记录（模拟 Loader 完成后）
        # ========================================
        print("Test 1: Insert file record (after Loader)")
        kf_id = repo.insert(
            kb_id=TEST_KB_ID,
            source_type='path',
            source_uri='data/raw/test_document.pdf',
            file_name='test_document.pdf',
            file_ext='.pdf',
            file_size=12345,
            parser_profile='pdf_default',  # ← Loader 知道用了哪个解析器
            version=1,                      # ← 默认版本
            custom_docs=0                   # ← 默认无自定义
        )
        print(f"  ✅ Inserted kf_id: {kf_id}")
        print()
        
        # ========================================
        # 测试2：查询文件（初始状态）
        # ========================================
        print("Test 2: Get file by ID (initial state)")
        file_info = repo.get_by_id(kf_id)
        print(f"  ✅ File info:")
        print(f"     - kf_id: {file_info['kf_id']}")
        print(f"     - kb_id: {file_info['kb_id']}")
        print(f"     - file_name: {file_info['file_name']}")
        print(f"     - status: {file_info['status']}")
        print(f"     - parser_profile: {file_info['parser_profile']}")
        print(f"     - version: {file_info['version']}")
        print(f"     - custom_docs: {file_info['custom_docs']}")
        print()
        
        # ========================================
        # 测试3：更新状态为 parsed（Loader 完成）
        # ========================================
        print("Test 3: Update status to 'parsed' (Loader completed)")
        repo.update_status(kf_id, 'parsed')
        file_info = repo.get_by_id(kf_id)
        print(f"  ✅ Status: {file_info['status']}")
        print(f"  ✅ Updated at: {file_info['updated_at']}")
        print()
        
        # ========================================
        # 测试4：更新状态为 chunked（Splitter 完成）
        # ========================================
        print("Test 4: Update status to 'chunked' (Splitter completed)")
        repo.update_status(
            kf_id=kf_id,
            status='chunked',
            chunk_count=10,
            chunk_profile='recursive_500_50'  # ← 新增：记录分块策略
        )
        file_info = repo.get_by_id(kf_id)
        print(f"  ✅ Status: {file_info['status']}")
        print(f"  ✅ Chunk count: {file_info['chunk_count']}")
        print(f"  ✅ Chunk profile: {file_info['chunk_profile']}")  # ← 新增
        print(f"  ✅ Updated at: {file_info['updated_at']}")
        print()
        
        # ========================================
        # 测试5：更新状态为 embedded（Embedding 完成）
        # ========================================
        print("Test 5: Update status to 'embedded' (Embedding completed)")
        repo.update_status(
            kf_id=kf_id,
            status='embedded',
            vector_count=10,                      # ← 修改：不再传 chunk_count
            embed_model='text-embedding-3-small'  # ← 新增：记录向量模型
        )
        file_info = repo.get_by_id(kf_id)
        print(f"  ✅ Status: {file_info['status']}")
        print(f"  ✅ Chunk count: {file_info['chunk_count']}")  # ← 应该保持 10（不变）
        print(f"  ✅ Vector count: {file_info['vector_count']}")
        print(f"  ✅ Embed model: {file_info['embed_model']}")  # ← 新增
        print(f"  ✅ Updated at: {file_info['updated_at']}")
        print()
        
        # ========================================
        # 测试6：查询知识库的所有文件
        # ========================================
        print("Test 6: Get all files in knowledge base")
        files = repo.get_by_kb_id(TEST_KB_ID)
        print(f"  ✅ Found {len(files)} file(s) in KB '测试知识库'")
        for f in files:
            status_emoji = {
                'pending': '⏳',
                'parsed': '📄',
                'chunked': '✂️',
                'embedded': '✅',
                'failed': '❌'
            }.get(f['status'], '❓')
            chunk_info = f"{f['chunk_count'] or 0} chunks"
            vector_info = f", {f['vector_count'] or 0} vectors" if f['vector_count'] else ""
            print(f"     {status_emoji} {f['file_name']}: {f['status']} ({chunk_info}{vector_info})")
        print()
        
        # ========================================
        # 测试7：测试失败状态
        # ========================================
        print("Test 7: Test 'failed' status")
        # 插入另一个文件用于测试失败状态
        kf_id_fail = repo.insert(
            kb_id=TEST_KB_ID,
            source_type='path',
            source_uri='data/raw/failed_document.pdf',
            file_name='failed_document.pdf',
            file_ext='.pdf',
            file_size=999
        )
        repo.update_status(kf_id_fail, 'failed')
        file_info_fail = repo.get_by_id(kf_id_fail)
        print(f"  ✅ Failed file status: {file_info_fail['status']}")
        print()
        
        print("="*60)
        print("✅ All tests passed!")
        print("="*60)
        print()
        print("Note: Test data inserted into database.")
        print("To clean up, run:")
        print(f"  DELETE FROM knowledge_file WHERE kf_id IN ('{kf_id}', '{kf_id_fail}');")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())