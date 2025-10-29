'''
职责：封装 file_doc 表的操作

功能（当前阶段极简版）：

insert(file_id, chunk_index, chunk_text, metadata)
插入单条 chunk 记录
生成 doc_id（UUID）
vector_id 暂时为 NULL（等有向量库后再填）
不做（暂时）：

批量插入（后续优化）
查询 chunks（后续 RAG 检索需要时再加）

'''