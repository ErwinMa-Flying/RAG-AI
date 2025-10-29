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
        # 测试1：插入文件记录
        print("Test 1: Insert file record")
        kf_id = repo.insert(
            kb_id=TEST_KB_ID,  # ← 使用真实的 kb_id
            source_type='path',
            source_uri='data/raw/test_document.pdf',
            file_name='test_document.pdf',
            file_ext='.pdf',
            file_size=12345
        )
        print(f"  ✅ Inserted kf_id: {kf_id}")
        print()
        
        # 测试2：查询文件
        print("Test 2: Get file by ID")
        file_info = repo.get_by_id(kf_id)
        print(f"  ✅ File info:")
        print(f"     - kf_id: {file_info['kf_id']}")
        print(f"     - kb_id: {file_info['kb_id']}")
        print(f"     - file_name: {file_info['file_name']}")
        print(f"     - status: {file_info['status']}")
        print(f"     - source_type: {file_info['source_type']}")
        print()
        
        # 测试3：更新状态为 parsed（Loader 完成）
        print("Test 3: Update status to 'parsed'")
        repo.update_status(kf_id, 'parsed')
        file_info = repo.get_by_id(kf_id)
        print(f"  ✅ Status: {file_info['status']}")
        print()
        
        # 测试4：更新状态为 chunked（Splitter 完成）
        print("Test 4: Update status to 'chunked'")
        repo.update_status(kf_id, 'chunked', chunk_count=5)
        file_info = repo.get_by_id(kf_id)
        print(f"  ✅ Status: {file_info['status']}")
        print(f"  ✅ Chunk count: {file_info['chunk_count']}")
        print()
        
        # 测试5：更新状态为 embedded（Embedding 完成）
        print("Test 5: Update status to 'embedded'")
        repo.update_status(kf_id, 'embedded', chunk_count=5, vector_count=5)
        file_info = repo.get_by_id(kf_id)
        print(f"  ✅ Status: {file_info['status']}")
        print(f"  ✅ Chunk count: {file_info['chunk_count']}")
        print(f"  ✅ Vector count: {file_info['vector_count']}")
        print(f"  ✅ Updated at: {file_info['updated_at']}")
        print()
        
        # 测试6：查询知识库的所有文件
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
            print(f"     {status_emoji} {f['file_name']}: {f['status']} (chunks: {f['chunk_count'] or 0})")
        print()
        
        print("="*60)
        print("✅ All tests passed!")
        print("="*60)
        print()
        print("Note: Test data inserted into database.")
        print("To clean up, run:")
        print(f"  DELETE FROM knowledge_file WHERE kf_id = '{kf_id}';")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())