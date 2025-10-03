import re
import unicodedata
import pandas as pd

# Existing functions ------------------------------------------------------

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


# Extended normalization helpers -----------------------------------------

ROMAN_MAP = {
    " i ": " 1 ",
    " ii ": " 2 ",
    " iii ": " 3 ",
    " iv ": " 4 ",
    " v ": " 5 ",
    " vi ": " 6 ",
    " vii ": " 7 ",
    " viii ": " 8 ",
    " ix ": " 9 ",
    " x ": " 10 ",
}

ARTICLES = {"the", "a", "an"}
FRANCHISE_WORDS = {"part", "episode", "chapter", "vol", "volume"}


def normalize_for_matching_extended(text: str) -> str:
    """
    Extended normalization for fuzzy matching.
    Builds on normalize_title_for_matching with extra steps:
      - Strip leading articles ("the", "a", "an")
      - Roman numeral → digit conversion
      - Franchise cruft removal ("part 2", "episode iv")
    """
    base = normalize_title_for_matching(text)
    if not base:
        return ""

    # Convert roman numerals
    s = f" {base} "
    for k, v in ROMAN_MAP.items():
        s = s.replace(k, v)
    s = s.strip()

    # Strip leading articles
    toks = s.split()
    while toks and toks[0] in ARTICLES:
        toks = toks[1:]
    s = " ".join(toks)

    # Remove franchise cruft
    s = re.sub(rf"\b({'|'.join(FRANCHISE_WORDS)})\s+\d+\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s