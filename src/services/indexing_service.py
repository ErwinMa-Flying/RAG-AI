'''
职责：封装文件索引的业务逻辑

功能（当前阶段极简版）：

index_file(file_path, kb_id)
步骤 1：调用 KnowledgeFileRepo.insert() 插入文件记录
步骤 2：调用 LangchainLoaderAdapter.load([file_path]) 读取文件
步骤 3：把每个 RawDoc 当作一个 chunk（暂不分块）
步骤 4：调用 FileDocRepo.insert() 插入 chunk 记录
步骤 5：调用 KnowledgeFileRepo.update_status() 更新为 completed
错误处理：捕获异常，更新状态为 failed
初始化参数：

kb_id（当前处理哪个知识库）
依赖注入：file_repo, doc_repo, loader
暂不做：

分块（等 Chunker 实现后再加）
Embedding（等后续再加）
向量库（等后续再加
'''