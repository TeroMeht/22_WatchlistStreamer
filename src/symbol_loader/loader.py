import os
import logging

logger = logging.getLogger(__name__)


def load_symbols_from_folder(folder_path: str) -> list[str]:
    """
    Reads all .txt files from the specified folder, extracts and cleans symbols.
    Examples:
        NASDAQ:CELH -> CELH
        NYSE:SNOW  -> SNOW
    Returns a deduplicated, sorted list of clean ticker symbols.
    """

    all_symbols = set()

    if not os.path.isdir(folder_path):
        logger.warning(f"Symbol folder '{folder_path}' not found.")
        return []

    for filename in os.listdir(folder_path):
        if not filename.endswith(".txt"):
            continue

        file_path = os.path.join(folder_path, filename)
        logger.info(f"Loading symbols from {file_path}")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                # Split by commas and newlines
                raw_symbols = [s.strip() for s in content.replace("\n", ",").split(",") if s.strip()]
                for sym in raw_symbols:
                    # Skip section headers like ###THIS WEEK
                    if sym.startswith("###") or not ":" in sym:
                        continue

                    # Strip exchange prefix (e.g., NASDAQ:, NYSE:)
                    clean_symbol = sym.split(":")[-1].strip().upper()
                    all_symbols.add(clean_symbol)
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")

    sorted_symbols = sorted(all_symbols)
    logger.info(f"Loaded {len(sorted_symbols)} unique clean symbols.")
    return sorted_symbols
