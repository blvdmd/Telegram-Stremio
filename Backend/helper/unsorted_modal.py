"""
Pydantic schemas for unsorted files.
Isolated from main media schemas to avoid merge conflicts with upstream.
"""

import re
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


# ===== MIME Type to Extension Mapping =====
MIME_TO_EXTENSION = {
    # Video
    "video/mp4": "mp4",
    "video/x-matroska": "mkv",
    "video/webm": "webm",
    "video/quicktime": "mov",
    "video/x-msvideo": "avi",
    "video/x-flv": "flv",
    "video/x-ms-wmv": "wmv",
    "video/mpeg": "mpg",
    "video/3gpp": "3gp",
    # Audio
    "audio/mpeg": "mp3",
    "audio/mp3": "mp3",
    "audio/flac": "flac",
    "audio/x-flac": "flac",
    "audio/wav": "wav",
    "audio/x-wav": "wav",
    "audio/ogg": "ogg",
    "audio/aac": "aac",
    "audio/mp4": "m4a",
    "audio/x-m4a": "m4a",
    "audio/opus": "opus",
    # Documents
    "application/pdf": "pdf",
    "text/plain": "txt",
    "application/json": "json",
    "text/html": "html",
    "text/xml": "xml",
    "application/xml": "xml",
    "text/csv": "csv",
    "application/msword": "doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    # Archives
    "application/zip": "zip",
    "application/x-zip-compressed": "zip",
    "application/x-rar-compressed": "rar",
    "application/vnd.rar": "rar",
    "application/x-7z-compressed": "7z",
    "application/gzip": "gz",
    "application/x-tar": "tar",
    "application/x-bzip2": "bz2",
    # Images
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "image/webp": "webp",
    "image/svg+xml": "svg",
}


class UnsortedFileSchema(BaseModel):
    """Schema for files that failed metadata lookup or are non-video."""
    
    db_index: int
    
    # File information
    file_name: str
    caption: Optional[str] = None
    file_extension: str
    mime_type: str
    media_type: str  # "archive", "audio", "document", "video", "other"
    
    # Size
    size: str  # Human readable (e.g., "3.37 GB")
    size_bytes: int
    
    # Media metadata (video/audio only)
    duration: Optional[int] = None  # Duration in seconds
    duration_formatted: Optional[str] = None  # Human readable (e.g., "2h 16m")
    width: Optional[int] = None  # Video width
    height: Optional[int] = None  # Video height
    quality: Optional[str] = None  # Detected quality (e.g., "1080p", "FLAC")
    
    # Telegram reference (for streaming URL)
    telegram_id: str  # Encoded string for streaming
    file_unique_id: str  # Telegram's permanent file ID
    
    # Source channel info (where file was forwarded FROM)
    source_channel_id: Optional[str] = None
    source_channel_name: Optional[str] = None
    source_channel_username: Optional[str] = None
    source_msg_id: Optional[int] = None
    source_msg_link: Optional[str] = None
    
    # Metadata
    processing_error: Optional[str] = None  # Why file ended up here
    created_on: datetime = Field(default_factory=datetime.utcnow)
    updated_on: datetime = Field(default_factory=datetime.utcnow)


# ===== Helper Functions =====

def get_media_type(mime_type: str, extension: str) -> str:
    """
    Determine media type category from MIME type and extension.
    
    Returns: "archive", "audio", "document", "video", or "other"
    """
    ext = extension.lower().lstrip(".")
    mime_lower = (mime_type or "").lower()
    
    # Audio
    if mime_lower.startswith("audio/") or ext in ["mp3", "flac", "wav", "aac", "ogg", "m4a", "wma", "opus"]:
        return "audio"
    
    # Archive - check for split file patterns too
    archive_exts = ["zip", "rar", "7z", "tar", "gz", "bz2", "xz", "iso"]
    if ext in archive_exts or "part" in ext.lower() or ext.isdigit():
        return "archive"
    
    # Document
    doc_exts = ["txt", "pdf", "doc", "docx", "py", "json", "xml", "csv", "md", "rtf", "html", "js", "css"]
    if ext in doc_exts:
        return "document"
    
    # Video (that failed metadata lookup)
    video_exts = ["mkv", "mp4", "avi", "mov", "wmv", "flv", "webm", "m4v", "ts"]
    if mime_lower.startswith("video/") or ext in video_exts:
        return "video"
    
    return "other"


def detect_quality(filename: str, caption: Optional[str], height: Optional[int], width: Optional[int]) -> Optional[str]:
    """
    Detect quality from filename, caption, or dimensions.
    
    Priority: Filename patterns -> Caption patterns -> Dimensions -> None
    
    Uses word boundaries to avoid false positives (e.g., "4k" inside "4kHdHub").
    Checks explicit resolution patterns (1080p, 720p) before ambiguous ones (4k, uhd).
    
    Returns: "4K", "1080p", "720p", "480p", "SD", "FLAC", "320kbps", or None
    """
    # Check filename and caption patterns first (most reliable when explicit)
    for text in [filename, caption]:
        if not text:
            continue
        fn = text.lower()
        
        # Check explicit resolution patterns first (most reliable)
        # Using word boundaries \b to avoid matching inside other words like "4kHdHub"
        if re.search(r'\b2160p\b', fn):
            return "4K"
        if re.search(r'\b1080p\b', fn) or re.search(r'\bfhd\b', fn) or re.search(r'\bfull\s*hd\b', fn):
            return "1080p"
        if re.search(r'\b720p\b', fn):
            return "720p"
        if re.search(r'\b480p\b', fn):
            return "480p"
        if re.search(r'\b360p\b', fn):
            return "SD"
        
        # Check 4K/UHD patterns last (more ambiguous, can appear in group names)
        # Only match standalone "4k" or "uhd", not as part of other words
        if re.search(r'\b4k\b', fn) or re.search(r'\buhd\b', fn):
            return "4K"
    
    # Fall back to dimensions (least reliable - could be upscaled)
    if height:
        if height >= 2160:
            return "4K"
        elif height >= 1080:
            return "1080p"
        elif height >= 720:
            return "720p"
        elif height >= 480:
            return "480p"
        else:
            return "SD"
    
    # Audio quality patterns (check both filename and caption)
    for text in [filename, caption]:
        if not text:
            continue
        fn = text.lower()
        if re.search(r'\.flac\b', fn) or re.search(r'\bflac\b', fn):
            return "FLAC"
        if re.search(r'\b320\s*kbps\b', fn):
            return "320kbps"
        if re.search(r'\b256\s*kbps\b', fn):
            return "256kbps"
    
    return None


def get_extension_from_mime(mime_type: str) -> Optional[str]:
    """
    Get file extension from MIME type.
    
    Returns extension without dot (e.g., "mp4") or None if unknown.
    """
    if not mime_type:
        return None
    return MIME_TO_EXTENSION.get(mime_type.lower())


def format_duration(seconds: Optional[int]) -> Optional[str]:
    """
    Format duration in seconds to H:MM:SS format.
    
    Examples:
        45 -> "0:45"
        125 -> "2:05"
        3665 -> "1:01:05"
        7200 -> "2:00:00"
    """
    if seconds is None or seconds <= 0:
        return None
    
    # Convert to int to handle float durations from Telegram
    seconds = int(seconds)
    
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"  # H:MM:SS
    else:
        return f"{minutes}:{secs:02d}"  # M:SS


def get_file_extension(filename: str) -> str:
    """
    Extract file extension from filename.
    Handles edge cases like .tar.gz and .part001
    """
    if not filename:
        return ""
    
    # Handle compound extensions
    lower = filename.lower()
    if lower.endswith(".tar.gz"):
        return "tar.gz"
    if lower.endswith(".tar.bz2"):
        return "tar.bz2"
    if lower.endswith(".tar.xz"):
        return "tar.xz"
    
    # Standard extension
    if "." in filename:
        return filename.rsplit(".", 1)[-1].lower()
    
    return ""

