from pathlib import Path
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_langchain_loader")

# 把 src 加到 sys.path，保证能用绝对导入
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# 优化后的候选路径（去除冗余，保留必要的变体）
LoaderClass = None
candidates = [
    # 当前实际路径（adaptors + langchain_Loader）
    ("adaptors.langchain.langchain_Loader", "LangchainLoaderAdapter"),
    
    # 文件名小写变体
    ("adaptors.langchain.langchain_loader", "LangchainLoaderAdapter"),
    
    # 从 __init__.py 导入（如果你在 __init__.py 中导出了）
    ("adaptors.langchain", "LangchainLoaderAdapter"),
    
    # 如果将来改成 adapters（备用）
    ("adapters.langchain.langchain_loader", "LangchainLoaderAdapter"),
]

for mod_name, cls_name in candidates:
    try:
        mod = __import__(mod_name, fromlist=[cls_name])
        LoaderClass = getattr(mod, cls_name)
        logger.info("Imported %s from %s", cls_name, mod_name)
        break
    except Exception:
        continue

if LoaderClass is None:
    logger.error("Failed to import Langchain loader. Check package name and file name under src/")
    raise SystemExit(1)

# 找一个用于测试的文件：优先 data/raw 下的第一个文件
raw_dir = PROJECT_ROOT / "data" / "raw"
test_file = None
if raw_dir.exists() and raw_dir.is_dir():
    for f in raw_dir.iterdir():
        if f.is_file():
            test_file = f
            break

# 退回到 testdata/sample.txt（如果没有 data/raw）
if test_file is None:
    fallback = PROJECT_ROOT / "testdata" / "sample.txt"
    if fallback.exists():
        test_file = fallback

if test_file is None or not test_file.exists():
    logger.error("No test file found in data/raw or testdata/sample.txt. Place a file and retry.")
    raise SystemExit(1)

logger.info("Testing with file: %s", test_file)

loader = LoaderClass()

# 尝试 run()，若失败则回退到 load()
docs_iter = None
try:
    docs_iter = loader.run(test_file)
except TypeError:
    try:
        docs_iter = loader.run([test_file])
    except Exception as e:
        logger.warning("run() failed (%s), will try load()", e)
        try:
            docs_iter = loader.load([test_file])
        except Exception as e2:
            logger.exception("load() also failed: %s", e2)
            raise SystemExit(1)
except Exception as e:
    logger.warning("run() raised, will try load(): %s", e)
    try:
        docs_iter = loader.load([test_file])
    except Exception as e2:
        logger.exception("load() also failed: %s", e2)
        raise SystemExit(1)

# docs_iter 可能是 generator 或 list
count = 0
for doc in docs_iter:
    text_preview = (getattr(doc, "text", "") or "")[:1000]
    metadata = getattr(doc, "metadata", {}) or {}
    print(f"\n--- RawDoc #{count} ---")
    print("text preview:")
    print(text_preview)
    print("metadata:", metadata)
    count += 1
    if count >= 5:
        break

if count == 0:
    logger.warning("No RawDoc yielded. Check loader, allowed_exts, and installed dependencies.")
else:
    logger.info("Done, yielded %d RawDoc(s).", count)