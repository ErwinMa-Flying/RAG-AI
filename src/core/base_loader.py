'''
这个文件定义“所有Loader的共同父类BaseLoader”
作用:规定统一接口(load),并提供可复用的通用工具(发现文件、保存JSON等)。
子类(TxtLoader/PdfLoader/MdLoader…)只需要关心“怎么读取并产出 RawDoc”。
'''
from __future__ import annotations #允许在类型注解中使用尚未定义的类型名

from abc import ABC, abstractmethod #ABC:抽象基类,abstractmethod:抽象方法装饰器
from pathlib import Path            #Path对象：统一存储路径（跨平台）
from typing import Iterable, Sequence, Dict, Any, Optional, List    #类型注解
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
        给定一个目录或单个文件路径，返回“当前 Loader 支持的所有文件列表”。
        - 若是目录：递归地查找所有允许的扩展名（allowed_exts）
        - 若是单个文件：仅当扩展名被支持时才返回
        """
        root = Path(root)  # 统一转为 Path 对象
        if root.is_file():  # 如果传入的是文件而非目录
            # 仅当该文件后缀在 allowed_exts 列表中时才返回
            return [root] if root.suffix in cls.allowed_exts() else []
        # 如果是目录，则递归查找所有符合条件的文件
        files: List[Path] = []
        for ext in cls.allowed_exts():
            files.extend(root.rglob(f"*{ext}"))  # rglob 递归匹配：**/*.ext
        return sorted(files)               # 返回排序后的结果，便于可预测的处理顺序