"""
Unsorted file receiver - handles non-video files and video files that fail metadata lookup.

This module provides a handler that can be called from the main receiver when:
1. A file is not a video (archive, audio, document, etc.)
2. A video file fails metadata lookup

Files are stored in the 'unsorted' collection for browsing via /files UI.
"""

from datetime import datetime
from typing import Optional

from pyrogram import Client
from pyrogram.types import Message

from Backend.logger import LOGGER
from Backend.helper.pyro import get_readable_file_size
from Backend.helper.encrypt import encode_string
from Backend.helper.unsorted_modal import (
    get_media_type,
    get_file_extension,
    get_extension_from_mime,
    detect_quality,
    format_duration,
    extract_best_filename
)


async def handle_unsorted_file(
    client: Client,
    message: Message,
    error: Optional[str] = None
) -> bool:
    """
    Handle a file that should go to the unsorted collection.
    
    Called when:
    - File is not a video (non-video MIME type)
    - Video file fails metadata lookup
    
    Args:
        client: Pyrogram client
        message: Message containing the file
        error: Optional error message (e.g., "Metadata lookup failed")
        
    Returns:
        True if file was saved successfully
    """
    try:
        # Import here to avoid circular imports
        from Backend import unsorted_collection
        
        # Get the file object (could be document, video, audio, etc.)
        file = (
            message.document or
            message.video or
            message.audio or
            message.voice or
            message.video_note or
            message.animation
        )
        
        if not file:
            LOGGER.warning("[UnsortedReceiver] No file attachment found in message")
            return False
        
        # Extract file information
        file_name = getattr(file, "file_name", None) or f"file_{message.id}"
        mime_type = getattr(file, "mime_type", None) or "application/octet-stream"
        file_size = getattr(file, "file_size", 0) or 0
        file_unique_id = getattr(file, "file_unique_id", "") or ""
        
        # Get duration and dimensions (video/audio only)
        duration = getattr(file, "duration", None)
        width = getattr(file, "width", None)
        height = getattr(file, "height", None)
        
        # Extract extension - if missing, infer from MIME type
        extension = get_file_extension(file_name)
        if not extension and mime_type:
            inferred_ext = get_extension_from_mime(mime_type)
            if inferred_ext:
                file_name = f"{file_name}.{inferred_ext}"
                extension = inferred_ext
        
        # Extract best filename from caption (done once at ingestion, saved to DB)
        file_name = extract_best_filename(file_name, message.caption, extension)
        
        # Determine media type from MIME and extension
        media_type = get_media_type(mime_type, extension)
        
        # Detect quality only for files with dimensions (video files)
        # Non-video files (archives, audio, documents) don't need quality indicators
        quality = None
        if height or width:
            quality = detect_quality(file_name, message.caption, height, width)
        
        # Format duration
        duration_formatted = format_duration(duration)
        
        # Build encoded telegram ID for streaming URL
        channel_id = str(message.chat.id).replace("-100", "")
        encoded_data = {
            "chat_id": channel_id,
            "msg_id": message.id
        }
        telegram_id = await encode_string(encoded_data)
        
        # Extract source channel info (if forwarded)
        source_channel_id = None
        source_channel_name = None
        source_channel_username = None
        source_msg_id = None
        source_msg_link = None
        
        if message.forward_from_chat:
            # File was forwarded from another channel
            forward_chat = message.forward_from_chat
            source_channel_id = str(forward_chat.id).replace("-100", "")
            source_channel_name = forward_chat.title
            source_channel_username = forward_chat.username
            source_msg_id = message.forward_from_message_id
            
            # Build source message link
            if source_channel_username:
                source_msg_link = f"https://t.me/{source_channel_username}/{source_msg_id}"
            elif source_channel_id:
                # Private channel link format
                source_msg_link = f"https://t.me/c/{source_channel_id}/{source_msg_id}"
        
        # Build the file document (capture forwarded files even if source is hidden)
        file_data = {
            "file_name": file_name,
            "caption": message.caption,
            "file_extension": extension,
            "mime_type": mime_type,
            "media_type": media_type,
            
            "size": get_readable_file_size(file_size),
            "size_bytes": file_size,
            
            "duration": duration,
            "duration_formatted": duration_formatted,
            "width": width,
            "height": height,
            "quality": quality,
            
            "telegram_id": telegram_id,
            "file_unique_id": file_unique_id,
            
            "source_channel_id": source_channel_id,
            "source_channel_name": source_channel_name,
            "source_channel_username": source_channel_username,
            "source_msg_id": source_msg_id,
            "source_msg_link": source_msg_link,
            
            "processing_error": error,
            
            # Convert to naive UTC datetime for consistent storage
            "created_on": datetime.utcfromtimestamp(message.date.timestamp()) if message.date else datetime.utcnow(),
            "updated_on": datetime.utcnow()
        }
        
        # Insert into database
        inserted_id = await unsorted_collection.insert_file(file_data)
        
        if inserted_id:
            LOGGER.info(
                f"[UnsortedReceiver] Saved unsorted file: {file_name} "
                f"({media_type}, {get_readable_file_size(file_size)})"
            )
            return True
        else:
            LOGGER.error(f"[UnsortedReceiver] Failed to save file: {file_name}")
            return False
            
    except Exception as e:
        LOGGER.exception(f"[UnsortedReceiver] Error handling file: {e}")
        return False


async def process_unsorted_message(client: Client, message: Message, stats: dict) -> bool:
    """
    Process a message for unsorted file collection (used by scan_unsorted).
    
    Similar to handle_unsorted_file but with stats tracking.
    
    Args:
        client: Pyrogram client
        message: Message to process
        stats: Dict to update with processing stats
        
    Returns:
        True if file was added
    """
    try:
        # Check if it has any file attachment
        file = (
            message.document or
            message.video or
            message.audio or
            message.voice or
            message.video_note or
            message.animation
        )
        
        if not file:
            return False
        
        # Determine if this is a video that should try main processing first
        mime_type = getattr(file, "mime_type", "") or ""
        is_video = message.video or mime_type.startswith("video/")
        
        if is_video:
            # This is a video - it should have gone through main receiver
            # Only add to unsorted if metadata failed (indicated by caller)
            error = "Metadata lookup failed"
        else:
            # Non-video file - add directly
            error = None
        
        result = await handle_unsorted_file(client, message, error)
        
        if result:
            stats["files_added"] += 1
            return True
        else:
            stats["failed"] += 1
            return False
            
    except Exception as e:
        LOGGER.error(f"[UnsortedReceiver] Error in process_unsorted_message: {e}")
        stats["errors"] += 1
        return False

