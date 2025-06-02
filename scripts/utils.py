# utils.py

import re
import pandas as pd

def is_mostly_digits(s, threshold=0.7):
    digits = sum(c.isdigit() for c in s)
    return digits / max(len(s), 1) > threshold

def normalize_title_for_matching(text):
    """
    Normalize soundtrack or movie titles:
    - Lowercase
    - Remove brackets, OST tags, subtitles
    - Strip noise words like "original soundtrack"
    """
    if not isinstance(text, str):
        return ""

    text = text.lower()

    noise_patterns = [
        r'\(.*?\)',
        r'\[.*?\]',
        r'original motion picture soundtrack',
        r'original soundtrack',
        r'motion picture soundtrack',
        r'complete motion picture score',
        r'deluxe edition',
        r'expanded edition',
        r'\bost\b',
        r'\bsoundtrack\b',
        r'\bscore\b'
    ]

    for pattern in noise_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)

    text = re.sub(r'[^a-z0-9\s]', '', text)  # remove punctuation
    return re.sub(r'\s+', ' ', text).strip()