import os
import logging

logger = logging.getLogger(__name__)



# ---------- Symbol parsing helpers ----------

def parse_symbols_from_text(content: str, source_name: str = "") -> set[str]:
    """
    Parse raw text content and extract clean uppercase ticker symbols.
    Supports both prefixed (NASDAQ:CELH) and plain (nvda,pltr) formats.
    """
    symbols = set()

    # Split by commas and newlines
    raw_symbols = [s.strip() for s in content.replace("\n", ",").split(",") if s.strip()]

    for sym in raw_symbols:
        # Skip section headers or comments
        if sym.startswith("###"):
            continue

        # Handle plain tickers (e.g., Userinput.txt)
        if ":" not in sym:
            clean = sym.upper()
            symbols.add(clean)
            continue

        # Handle exchange-prefixed tickers (e.g., NASDAQ:CELH)
        clean = sym.split(":")[-1].strip().upper()
        symbols.add(clean)

    logger.debug(f"Parsed {len(symbols)} symbols from {source_name or 'input'}")
    return symbols


# ---------- File reading helpers ----------

def read_symbols_from_file(file_path: str) -> set[str]:
    """
    Read one .txt file and extract ticker symbols using parse_symbols_from_text().
    Returns a set of uppercase tickers.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        return parse_symbols_from_text(content, os.path.basename(file_path))
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return set()


def get_text_files(folder_path: str) -> list[str]:
    """
    Return all .txt file paths inside a folder.
    """
    if not os.path.isdir(folder_path):
        logger.warning(f"Symbol folder '{folder_path}' not found.")
        return []
    return [
        os.path.join(folder_path, fn)
        for fn in os.listdir(folder_path)
        if fn.endswith(".txt")
    ]


# ---------- Main entry point ----------

def load_symbols_from_folder(folder_path: str) -> list[str]:
    """
    Load and combine symbols from all .txt files in the given folder.
    Returns a sorted list of unique uppercase ticker symbols.
    """
    all_symbols = set()

    for file_path in get_text_files(folder_path):
        logger.info(f"Loading symbols from {file_path}")
        symbols = read_symbols_from_file(file_path)
        all_symbols.update(symbols)

    sorted_symbols = sorted(all_symbols)
    logger.info(f"Loaded {len(sorted_symbols)} unique symbols: {sorted_symbols}")
    return sorted_symbols



