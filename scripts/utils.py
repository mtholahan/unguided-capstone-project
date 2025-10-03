# utils.py

import re
import unicodedata
import pandas as pd


def normalize_title_for_matching(text: str) -> str:
    """
    Normalize soundtrack or movie titles for robust fuzzy matching.

    Steps:
      1. Lowercase
      2. Strip diacritics (accents)
      3. Remove bracketed text: "(Deluxe)", "[Remix]", "{Special Edition}"
      4. Remove common noise phrases (OST, "Original Soundtrack", etc.)
      5. Remove punctuation/symbols
      6. Collapse multiple spaces
      7. Drop single-character tokens
      8. Return cleaned string
    """
    if not isinstance(text, str):
        return ""

    # 1) Lowercase + trim
    text = text.lower().strip()

    # 2) Normalize Unicode + strip accents
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))

    # 3) Remove bracketed content
    text = re.sub(r"\(.*?\)", " ", text)
    text = re.sub(r"\[.*?\]", " ", text)
    text = re.sub(r"\{.*?\}", " ", text)

    # 4) Noise phrases (soundtrack clutter)
    noise_patterns = [
        r"original motion picture soundtrack",
        r"original soundtrack",
        r"motion picture soundtrack",
        r"complete motion picture score",
        r"deluxe edition",
        r"expanded edition",
        r"\bost\b",
        r"\bsoundtrack\b",
        r"\bscore\b",
    ]
    for pat in noise_patterns:
        text = re.sub(pat, " ", text, flags=re.IGNORECASE)

    # 5) Remove non-alphanumeric chars
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    # 6) Collapse spaces
    text = re.sub(r"\s+", " ", text).strip()

    # 7) Drop single-character tokens
    tokens = [t for t in text.split() if len(t) > 1]
    return " ".join(tokens)


def clean_title(text: str) -> str:
    """
    Lightweight cleaner for display/logging.
    Keeps alphanumeric + spaces, strips obvious OST noise.
    """
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"\(.*?\)|\[.*?\]", "", text)
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def is_mostly_digits(s: str, threshold: float = 0.7) -> bool:
    """
    Return True if a string is mostly digits (like catalog IDs).
    """
    if not s:
        return False
    digits = sum(c.isdigit() for c in s)
    return digits / max(len(s), 1) > threshold
