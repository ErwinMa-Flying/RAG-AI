from pathlib import Path
from typing import Sequence, Optional, Dict, Iterable, Any
import logging

# 以绝对导入 core，确保运行时 PYTHONPATH 包含 src
from core.base_loader import BaseLoader
from core.types import RawDoc

logger = logging.getLogger(__name__)


class LangchainLoaderAdapter(BaseLoader):
    """
    LangChain adapter loader: 按文件后缀选择合适的 LangChain loader，
    将 LangChain Document 映射成项目的 RawDoc 并以 generator 流式产出。
    延迟导入具体 LangChain loader，以避免在不需要时强制安装依赖。
    """

    @classmethod
    def allowed_exts(cls) -> tuple[str, ...]:
        return (".txt", ".pdf", ".md", ".docx", ".html")

    def __init__(self, loader_kwargs: Optional[Dict[str, Any]] = None):
        self.loader_kwargs = dict(loader_kwargs) if loader_kwargs else {}
        self._loader_factories: Dict[str, Optional[callable]] = {}
        
        # 默认 encoding 用于文本类文件
        self._default_text_encoding = "utf-8"

    def _get_kwargs_for_loader(self, ext: str) -> Dict[str, Any]:
        """根据文件类型返回合适的 kwargs"""
        # 二进制格式不需要 encoding
        binary_formats = {".pdf", ".docx"}
        
        if ext in binary_formats:
            return {k: v for k, v in self.loader_kwargs.items() if k != "encoding"}
        else:
            # 文本格式，添加默认 encoding
            kwargs = self.loader_kwargs.copy()
            if "encoding" not in kwargs:
                kwargs["encoding"] = self._default_text_encoding
            return kwargs

    def _probe_and_cache_factory(self, ext: str):
        """探测并缓存 ext 对应的 loader factory（只做一次）。"""
        ext = ext.lower()
        if ext in self._loader_factories:
            return self._loader_factories[ext] # 已探测过

        factory = None
        loader_kwargs = self._get_kwargs_for_loader(ext)
        
        try:
            if ext == ".txt":
                try:
                    from langchain_community.document_loaders import TextLoader
                except ImportError:
                    from langchain.document_loaders import TextLoader
                factory = lambda p: TextLoader(str(p), **loader_kwargs)
            
            elif ext == ".pdf":
                try:
                    from langchain_community.document_loaders import PyPDFLoader
                except ImportError:
                    from langchain.document_loaders import PyPDFLoader
                factory = lambda p: PyPDFLoader(str(p), **loader_kwargs)

            elif ext == ".md":
                try:
                    from langchain_community.document_loaders import UnstructuredMarkdownLoader
                    factory = lambda p: UnstructuredMarkdownLoader(str(p), **loader_kwargs)
                except ImportError:
                    try:
                        from langchain_community.document_loaders import MarkdownLoader
                    except ImportError:
                        from langchain.document_loaders import MarkdownLoader
                    factory = lambda p: MarkdownLoader(str(p), **loader_kwargs)

            elif ext == ".docx":
                try:
                    from langchain_community.document_loaders import UnstructuredWordDocumentLoader
                    factory = lambda p: UnstructuredWordDocumentLoader(str(p), **loader_kwargs)
                except ImportError:
                    try:
                        from langchain_community.document_loaders import Docx2txtLoader
                    except ImportError:
                        from langchain.document_loaders import Docx2txtLoader
                    factory = lambda p: Docx2txtLoader(str(p), **loader_kwargs)

            elif ext == ".html":
                try:
                    from langchain_community.document_loaders import BSHTMLLoader
                except ImportError:
                    from langchain.document_loaders import BSHTMLLoader
                factory = lambda p: BSHTMLLoader(str(p), **loader_kwargs)

        except ImportError as e:
            logger.debug(f"Failed to import loader for {ext}: {e}")
            factory = None

        self._loader_factories[ext] = factory
        return factory

    def _get_loader_for_path(self, path: Path):
        """为给定路径返回对应的 LangChain loader 实例（或 None）。"""
        ext = path.suffix.lower()
        factory = self._probe_and_cache_factory(ext)
        if factory is None:
            logger.warning(f"no langchain loader for {path}, skipping")
            return None
        return factory(path)

    def load(self, paths: Sequence[Path]) -> Iterable[RawDoc]:
        """
        遍历路径列表，为每个路径获取 loader 并读取 LangChain Document，
        映射为 RawDoc 并流式产出。
        """
        for path in paths:
            loader_instance = self._get_loader_for_path(path)
            if loader_instance is None:
                continue

            try:
                lc_docs = loader_instance.load()
                for lc_doc in lc_docs:
                    yield RawDoc(
                        text=lc_doc.page_content,
                        metadata={
                            "source_path": str(path),
                            **lc_doc.metadata
                        }
                    )
            except Exception as e:
                logger.exception(f"Error loading {path}: {e}")
                continue