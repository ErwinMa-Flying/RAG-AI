'''
职责：Pipeline 脚本，协调整个索引流程

功能：

读取命令行参数：kb_id（知识库 ID）
扫描 data/raw/{kb_id}/ 目录下的所有文件
创建 IndexingService 实例
循环调用 service.index_file(file, kb_id)
打印进度和统计（成功/失败文件数）
'''