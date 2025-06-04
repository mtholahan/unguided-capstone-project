# utils.py

import re
import pandas as pd

def is_mostly_digits(s, threshold=0.7):
    digits = sum(c.isdigit() for c in s)
    return digits / max(len(s), 1) > threshold

import re
import unicodedata

def normalize_title_for_matching(text: str) -> str:
    """
    Normalize soundtrack or movie titles by:
      - Lowercasing
      - Stripping diacritics (accents)
      - Removing bracketed text (e.g. "(Deluxe)", "[2005 Remaster]")
      - Removing common noise phrases (OST tags, "Original Soundtrack", etc.)
      - Removing punctuation
      - Collapsing multiple spaces
      - Dropping single-letter tokens
    """
    if not isinstance(text, str):
        return ""

    # 1) Lowercase and strip leading/trailing whitespace
    text = text.lower().strip()

    # 2) Normalize Unicode to NFKD and drop diacritics (accents)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))

    # 3) Remove bracketed content like "(Deluxe)" or "[Remix]"
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"\[.*?\]", "", text)

    # 4) Remove common “noise” phrases related to soundtracks
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
        text = re.sub(pat, "", text, flags=re.IGNORECASE)

    # 5) Remove any remaining non-alphanumeric characters (punctuation, symbols)
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    # 6) Collapse multiple spaces into one
    text = re.sub(r"\s+", " ", text).strip()

    # 7) Drop single-letter tokens (e.g. “a”, “x”)
    tokens = [t for t in text.split() if len(t) > 1]
    return " ".join(tokens)
