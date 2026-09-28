"""
Thin re-export of the one existing page-path normalizer
(pipeline/rollups.py's _normalize_page_path) - reused, not reimplemented,
per the spec's explicit "do not create multiple incompatible URL
normalization implementations" instruction. pipeline/rollups.py is
imported from here, never modified.
"""

from pipeline.rollups import _normalize_page_path as normalize_page_path

__all__ = ["normalize_page_path"]
