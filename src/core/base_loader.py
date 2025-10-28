'''
这个文件定义“所有Loader的共同父类BaseLoader”
作用:规定统一接口(load),并提供可复用的通用工具(发现文件、保存JSON等)。
子类(TxtLoader/PdfLoader/MdLoader…)只需要关心“怎么读取并产出 RawDoc”。
'''
from __future__ import annotations #允许在类型注解中使用尚未定义的类型名

from abc import ABC, abstractmethod #ABC:抽象基类,abstractmethod:抽象方法装饰器
from pathlib import Path            #Path对象：统一存储路径（跨平台）
from typing import Iterable, Sequence, Dict, Any, Optional, List, Union    #类型注解
import json                        #JSON读写

from .types import RawDoc  #导入RawDoc数据类

class BaseLoader(ABC):
    """
    抽象父类：把“若干文件路径”转换为“若干 RawDoc”。
    上层只依赖这个父类；具体文件格式逻辑由子类实现（例如 TxtLoader/PdfLoader）。
    """
    # ① 声明：当前 Loader 支持的文件扩展名
    #    - 这是一个“类方法”，子类可以覆写（例如 PdfLoader 返回 (".pdf",)）
    @classmethod
    def allowed_exts(cls) -> tuple[str, ...]:
        # 默认只支持 .txt，作为示例；具体 Loader 应该覆写这个方法
        return (".txt",)
    
    # ② 抽象方法：子类必须实现“怎么读取并产生 RawDoc”
    @abstractmethod
    def load(
        self,
        paths: Sequence[Path],
        options: Optional[Dict[str, Any]] = None,
    ) -> Iterable[RawDoc]:
        """
        读取给定文件，产出 RawDoc 序列（不负责写文件）。
        - 只做“读 + 组装 RawDoc”的职责；落盘由基类的 save() 处理。
        - 子类必须实现本方法。
        """
        raise NotImplementedError #抽象方法：不实现；强制子类去实现
    
    # ③ 通用工具：在一个目录下发现“本 Loader 支持的所有文件”
    @classmethod
    def discover_files(cls, root: Path | str) -> List[Path]:
        """
        返回当前 Loader 支持的所有文件（递归）。
        - 规范化 allowed_exts 到小写且以 '.' 开头
        - 若 root 是文件且后缀匹配则返回该文件
        - 若 root 是目录则一次遍历并按后缀过滤（去重 & 排序）
        - 若 root 不存在则返回空列表
        """
        root = Path(root)  # 统一转为 Path 对象
        if not root.exists():  # 若路径不存在则返回空列表
            return []
        
        #归一化允许的扩展名（确保以'.'开头并小写）
        exts = {(e if e.startswith('.') else f".{e}").lower() for e in cls.allowed_exts()}

        if root.is_file():  # 如果传入的是文件而非目录
            # 仅当该文件后缀在 allowed_exts 列表中时才返回
            return [root] if root.suffix.lower() in exts else []
        
        # 一次遍历目录并按后缀过滤，避免多次rglob带来的重复/性能问题
        files: List[Path] = []
        for p in root.rglob("*"):  # 递归遍历目录下所有文件
            if p.is_file() and p.suffix.lower() in exts:
                files.append(p)

        # 去重并排序(Path 可比较)
        unique = sorted(set(files))
        return unique
    
     # ④ 通用工具：把 RawDoc 转成“可 JSON 序列化”的 dict（便于落盘）
    @staticmethod
    def to_serializable(doc: RawDoc) -> Dict[str, Any]:
        """
        RawDoc -> dict
        - JSON 不能直接存 dataclass；需要转成纯 dict（text + metadata）
        """
        return {
            "text": doc.text,
            "metadata": dict(doc.metadata) # 确保 metadata 是普通字典
        }
    
    # ⑤ 通用工具：为某个 RawDoc 推断一个合理的输出文件路径
    @staticmethod
    def default_out_path(doc: RawDoc, out_dir: Path) -> Path:
        """
        生成输出文件路径：
        - 优先使用 metadata['source_path'] 的“文件名（不含扩展名）”
        - 若没有 source_path，则退化到使用 metadata['doc_id']
        - 输出扩展名固定为 .docjson（我们约定的 RawDoc 落盘格式）
        """
        stem = None #文件名（不含扩展名）
        src = doc.metadata.get("source_path") #尝试从 metadata 取 source_path
        if isinstance(src, str):              #如果拿到了有效字符串
            stem = Path(src).stem               #取文件名（不含扩展名）
        if not stem: #如果没有 source_path，则退化到 doc_id
            stem = str(doc.metadata.get("doc_id", "rawdoc")) #默认名为 rawdoc
        return out_dir / f"{stem}.docjson" #拼接输出路径
    
    # ⑥ 通用工具：批量把 RawDoc 写成 .docjson 文件
    # 流式场景下无需调用
    @classmethod
    def save(
        cls,
        docs: Iterable[RawDoc],
        out_dir: Path | str,
    ) -> List[Path]:
        """
        将一批 RawDoc 序列化为 JSON，写入 out_dir 目录，返回写出的文件路径列表。
        - 原子写入（先写临时文件再替换）
        - 单文件异常捕获，继续处理其它文档
        """
        import os
        import tempfile

        out = Path(out_dir)  # 统一转为 Path 对象
        out.mkdir(parents=True, exist_ok=True)  # 确保输出目录存在；若不存在则递归创建，已存在则不报错。
        written: list[Path] = []  # 记录写出的文件路径

        for doc in docs:
            out_path = cls.default_out_path(doc, out)  # 推断输出路径
            tmp_file = None
            try:
                #在目标目录创建临时文件以保证文件系统内可原子替换
                fd, tmp_file = tempfile.mkstemp(dir=str(out_path.parent))
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(
                    cls.to_serializable(doc),  # 转成可序列化 dict
                    f,
                    ensure_ascii=False,         # 保持非 ASCII 字符
                    indent=2                    # 美化缩进
                    )
                #原子替换：用临时文件覆盖目标文件
                os.replace(tmp_file, str(out_path))
                written.append(out_path)  # 记录写出的文件路径
                tmp_file = None      #避免 finally 中删除已移动的文件
            except Exception:
                #记录/处理错误：这里简单忽略并确保临时文件被清理
                if tmp_file and Path(tmp_file).exists():
                    try:
                        Path(tmp_file).unlink()  # 删除临时文件
                    except Exception:
                        pass
                continue  # 继续处理下一个文档

        return written
    
    # ⑦ 一键运行：发现文件 → 加载 → （可选）保存（不传out_dir就不存docjson）
    def run(
        self,
        in_paths_or_dir: Sequence[Path | str] | Path | str,
        out_dir: Optional[Path | str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Union[Iterable[RawDoc], List[Path]]:
        """
        一键执行流程：发现 -> 加载 ->（可选）保存
        返回：
          - 若 out_dir 为 None：返回 Iterable[RawDoc]（streaming，不会立即 materialize）
          - 若指定 out_dir：返回写出的文件路径列表 List[Path]（一般不保存，直接走pipeline）
        """
        # Step 1: 规范化输入并展开目录（单个或序列中的目录都会展开）
        candidates: List[Path] = []
        if isinstance(in_paths_or_dir, (str, Path)):
            p = Path(in_paths_or_dir)
            if p.exists() and p.is_dir():
                candidates.extend(self.discover_files(p))
            else:
                candidates.append(p)
        else:
            for p in in_paths_or_dir:
                pth = Path(p)
                if pth.exists() and pth.is_dir():
                    candidates.extend(self.discover_files(pth))
                else:
                    candidates.append(pth)

        # Step 2: 归一化 allowed_exts 并过滤存在性与后缀
        exts = { (e if e.startswith('.') else f".{e}").lower() for e in self.allowed_exts() }
        valid_paths: List[Path] = []
        # 可以收集 skipped 用于日志/返回
        skipped: List[tuple[str, str]] = []
        for p in candidates:
            if not p.exists() or not p.is_file():
                skipped.append((str(p), "not found"))
                continue
            if p.suffix.lower() not in exts:
                skipped.append((str(p), "unsupported ext"))
                continue
            valid_paths.append(p)

        # Step 3: 调用子类实现的 load() 读取 RawDoc（可能是 generator）
        docs = self.load(valid_paths, options=options)

        # Step 4: 如果没指定 out_dir，仅返回数据（内存模式）
        if out_dir is None:
            # materialize：注意大数据会占内存，必要时改为直接返回 iterable
            return docs

        # Step 5: 写入文件并返回写出的路径
        return self.save(docs, Path(out_dir))