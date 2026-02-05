"""
Shared maintenance logic for tidy and scan operations.
Can be called from both Telegram bot commands and web API.
"""

import time
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Set, Tuple, List
from dataclasses import dataclass, field

from pyrogram.errors import FloodWait

from Backend import db, unsorted_collection
from Backend.config import Telegram
from Backend.helper.encrypt import decode_string
from Backend.helper.pyro import clean_filename, get_readable_file_size, remove_urls, to_utc_isoformat
from Backend.helper.metadata import metadata
from Backend.helper.task_manager import delete_message
from Backend.logger import LOGGER


# -------------------------------
# Progress State Management
# -------------------------------
@dataclass
class OperationProgress:
    """Tracks progress of a maintenance operation"""
    operation_type: str  # "tidy" or "scan"
    status: str = "idle"  # idle, running, completed, cancelled, error
    current_step: str = ""
    total_items: int = 0
    processed_items: int = 0
    checked: int = 0
    removed: int = 0
    videos_removed: int = 0
    unsorted_removed: int = 0
    movies_added: int = 0
    tv_added: int = 0
    unsorted_added: int = 0
    already_processed: int = 0
    already_processed_videos: int = 0
    already_processed_unsorted: int = 0
    skipped_no_file: int = 0
    failed_metadata: int = 0
    errors: int = 0
    start_time: float = 0
    end_time: float = 0
    error_message: str = ""
    channel_name: str = ""
    mode_label: str = ""
    tidy_target: str = "both"  # "both", "videos", "unsorted"
    
    def to_dict(self) -> dict:
        elapsed = (self.end_time or time.time()) - self.start_time if self.start_time else 0
        return {
            "operation_type": self.operation_type,
            "status": self.status,
            "current_step": self.current_step,
            "total_items": self.total_items,
            "processed_items": self.processed_items,
            "checked": self.checked,
            "removed": self.removed,
            "videos_removed": self.videos_removed,
            "unsorted_removed": self.unsorted_removed,
            "movies_added": self.movies_added,
            "tv_added": self.tv_added,
            "unsorted_added": self.unsorted_added,
            "already_processed": self.already_processed,
            "already_processed_videos": self.already_processed_videos,
            "already_processed_unsorted": self.already_processed_unsorted,
            "skipped_no_file": self.skipped_no_file,
            "failed_metadata": self.failed_metadata,
            "errors": self.errors,
            "elapsed_seconds": elapsed,
            "channel_name": self.channel_name,
            "mode_label": self.mode_label,
            "tidy_target": self.tidy_target,
            "error_message": self.error_message,
        }


# Global operation state with lock for thread safety
_operation_lock = asyncio.Lock()
_current_operation: Optional[OperationProgress] = None
_cancel_requested: bool = False
_pending_delete_tasks: List[asyncio.Task] = []


def get_current_operation() -> Optional[OperationProgress]:
    return _current_operation


def is_operation_running() -> bool:
    return _current_operation is not None and _current_operation.status == "running"


def request_cancel():
    global _cancel_requested
    _cancel_requested = True


def is_cancel_requested() -> bool:
    return _cancel_requested


def _track_delete_task(chat_id: int, msg_id: int):
    """Create and track a delete message task to prevent GC."""
    global _pending_delete_tasks
    try:
        task = asyncio.create_task(delete_message(chat_id, msg_id))
        _pending_delete_tasks.append(task)
    except Exception as e:
        LOGGER.debug(f"Could not create delete task for {msg_id}: {e}")


# -------------------------------
# Tidy Core Logic
# -------------------------------
async def is_message_valid(bot, telegram_id: str) -> Tuple[bool, int, int]:
    """
    Check if a telegram message exists and contains valid media.
    Returns: (is_valid, chat_id, msg_id)
    """
    try:
        decoded = await decode_string(telegram_id)
        chat_id = int(f"-100{decoded['chat_id']}")
        msg_id = int(decoded['msg_id'])
        
        message = await bot.get_messages(chat_id, msg_id)
        
        if message and (message.video or message.document):
            return (True, chat_id, msg_id)
        else:
            return (False, chat_id, msg_id)
            
    except Exception as e:
        LOGGER.debug(f"Error checking message validity: {e}")
        return (False, 0, 0)


async def tidy_movie(bot, collection, movie: dict, progress: OperationProgress) -> dict:
    """Check and clean up invalid telegram entries for a movie."""
    stats = {"checked": 0, "removed": 0}
    tmdb_id = movie.get("tmdb_id")
    telegram_list = movie.get("telegram", [])
    
    if not telegram_list:
        return stats
    
    valid_entries = []
    
    for telegram_item in telegram_list:
        if _cancel_requested:
            break
            
        stats["checked"] += 1
        telegram_id = telegram_item.get("id")
        
        if not telegram_id:
            stats["removed"] += 1
            continue
        
        is_valid, chat_id, msg_id = await is_message_valid(bot, telegram_id)
        
        if is_valid:
            valid_entries.append(telegram_item)
        else:
            stats["removed"] += 1
            LOGGER.info(f"[Tidy][Movie] Removing invalid entry: {movie.get('title')} - {telegram_item.get('quality')}")
            
            if chat_id and msg_id:
                _track_delete_task(chat_id, msg_id)
    
    if stats["removed"] > 0:
        try:
            if valid_entries:
                await collection.update_one(
                    {"tmdb_id": tmdb_id},
                    {"$set": {"telegram": valid_entries}}
                )
            else:
                await collection.delete_one({"tmdb_id": tmdb_id})
                LOGGER.info(f"[Tidy][Movie] Deleted movie with no valid entries: {movie.get('title')}")
        except Exception as e:
            LOGGER.error(f"[Tidy][Movie] DB update failed for {movie.get('title')}: {e}")
    
    return stats


async def tidy_tv_show(bot, collection, tv_show: dict, progress: OperationProgress) -> dict:
    """Check and clean up invalid telegram entries for a TV show."""
    stats = {"checked": 0, "removed": 0}
    tmdb_id = tv_show.get("tmdb_id")
    seasons = tv_show.get("seasons", [])
    modified = False
    
    for season in seasons:
        if _cancel_requested:
            break
            
        episodes_to_keep = []
        
        for episode in season.get("episodes", []):
            if _cancel_requested:
                break
                
            telegram_list = episode.get("telegram", [])
            valid_entries = []
            
            for telegram_item in telegram_list:
                if _cancel_requested:
                    break
                    
                stats["checked"] += 1
                telegram_id = telegram_item.get("id")
                
                if not telegram_id:
                    stats["removed"] += 1
                    modified = True
                    continue
                
                is_valid, chat_id, msg_id = await is_message_valid(bot, telegram_id)
                
                if is_valid:
                    valid_entries.append(telegram_item)
                else:
                    stats["removed"] += 1
                    modified = True
                    LOGGER.info(
                        f"[Tidy][TV] Removing invalid entry: {tv_show.get('title')} "
                        f"S{season.get('season_number')}E{episode.get('episode_number')}"
                    )
                    
                    if chat_id and msg_id:
                        _track_delete_task(chat_id, msg_id)
            
            if valid_entries:
                episode["telegram"] = valid_entries
                episodes_to_keep.append(episode)
        
        season["episodes"] = episodes_to_keep
    
    seasons = [s for s in seasons if s.get("episodes")]
    
    if modified:
        try:
            if seasons:
                await collection.update_one(
                    {"tmdb_id": tmdb_id},
                    {"$set": {"seasons": seasons}}
                )
            else:
                await collection.delete_one({"tmdb_id": tmdb_id})
                LOGGER.info(f"[Tidy][TV] Deleted TV show with no valid entries: {tv_show.get('title')}")
        except Exception as e:
            LOGGER.error(f"[Tidy][TV] DB update failed for {tv_show.get('title')}: {e}")
    
    return stats


async def is_unsorted_message_valid(bot, telegram_id: str) -> Tuple[bool, int, int]:
    """
    Check if a telegram message for unsorted file still exists.
    Returns: (is_valid, chat_id, msg_id)
    """
    try:
        decoded = await decode_string(telegram_id)
        chat_id = int(f"-100{decoded['chat_id']}")
        msg_id = int(decoded['msg_id'])
        
        message = await bot.get_messages(chat_id, msg_id)
        
        # For unsorted, accept any file attachment (not just video)
        if message and (message.document or message.video or message.audio or 
                       message.voice or message.video_note or message.animation):
            return (True, chat_id, msg_id)
        else:
            return (False, chat_id, msg_id)
            
    except Exception as e:
        LOGGER.debug(f"[Tidy][Unsorted] Error checking message: {e}")
        return (False, 0, 0)


async def tidy_unsorted_file(bot, collection, file_doc: dict, progress: OperationProgress) -> dict:
    """Check and clean up an unsorted file entry."""
    stats = {"checked": 1, "removed": 0}
    
    telegram_id = file_doc.get("telegram_id")
    if not telegram_id:
        await collection.delete_one({"_id": file_doc["_id"]})
        stats["removed"] = 1
        return stats
    
    is_valid, chat_id, msg_id = await is_unsorted_message_valid(bot, telegram_id)
    
    if not is_valid:
        await collection.delete_one({"_id": file_doc["_id"]})
        stats["removed"] = 1
        LOGGER.info(f"[Tidy][Unsorted] Removed: {file_doc.get('file_name')}")
        
        if chat_id and msg_id:
            _track_delete_task(chat_id, msg_id)
    
    return stats


async def run_tidy(bot, target: str = "both") -> OperationProgress:
    """
    Run the tidy operation using the provided bot client.
    
    Args:
        bot: Pyrogram Client instance
        target: "both", "videos", or "unsorted"
        
    Returns:
        progress object with results
    """
    global _current_operation, _cancel_requested, _pending_delete_tasks
    
    async with _operation_lock:
        if is_operation_running():
            raise RuntimeError("Another operation is already running")
        
        _cancel_requested = False
        _pending_delete_tasks = []  # Clear any old tasks
        _current_operation = OperationProgress(operation_type="tidy")
        _current_operation.status = "running"
        _current_operation.start_time = time.time()
        _current_operation.current_step = "Counting items..."
        _current_operation.tidy_target = target
    
    target_labels = {
        "both": "Videos + Unsorted Files",
        "videos": "Videos Only",
        "unsorted": "Unsorted Files Only"
    }
    _current_operation.mode_label = target_labels.get(target, target)
    
    try:
        # Count totals based on target
        total_movies = 0
        total_tv = 0
        total_unsorted = 0
        
        for i in range(1, db.current_db_index + 1):
            key = f"storage_{i}"
            if target in ("both", "videos"):
                total_movies += await db.dbs[key]["movie"].count_documents({})
                total_tv += await db.dbs[key]["tv"].count_documents({})
            if target in ("both", "unsorted"):
                total_unsorted += await unsorted_collection.dbs[key][unsorted_collection.COLLECTION_NAME].count_documents({})
        
        _current_operation.total_items = total_movies + total_tv + total_unsorted
        _current_operation.current_step = "Processing..."
        
        # Match bot command's concurrency settings
        CONCURRENCY = 5
        RATE_LIMIT_DELAY = 0.5
        semaphore = asyncio.Semaphore(CONCURRENCY)
        
        # Shared counters for concurrent tasks
        checked_count = 0
        removed_count = 0
        videos_removed = 0
        unsorted_removed = 0
        processed_count = 0
        
        async def process_movie_concurrent(collection, movie):
            nonlocal checked_count, removed_count, videos_removed, processed_count
            if _cancel_requested:
                return
            async with semaphore:
                stats = await tidy_movie(bot, collection, movie, _current_operation)
                checked_count += stats["checked"]
                removed_count += stats["removed"]
                videos_removed += stats["removed"]
                processed_count += 1
                await asyncio.sleep(RATE_LIMIT_DELAY)
        
        async def process_tv_concurrent(collection, tv_show):
            nonlocal checked_count, removed_count, videos_removed, processed_count
            if _cancel_requested:
                return
            async with semaphore:
                stats = await tidy_tv_show(bot, collection, tv_show, _current_operation)
                checked_count += stats["checked"]
                removed_count += stats["removed"]
                videos_removed += stats["removed"]
                processed_count += 1
                await asyncio.sleep(RATE_LIMIT_DELAY)
        
        async def process_unsorted_concurrent(collection, file_doc):
            nonlocal checked_count, removed_count, unsorted_removed, processed_count
            if _cancel_requested:
                return
            async with semaphore:
                stats = await tidy_unsorted_file(bot, collection, file_doc, _current_operation)
                checked_count += stats["checked"]
                removed_count += stats["removed"]
                unsorted_removed += stats["removed"]
                processed_count += 1
                await asyncio.sleep(RATE_LIMIT_DELAY)
        
        # Process databases
        for i in range(1, db.current_db_index + 1):
            if _cancel_requested:
                break
                
            key = f"storage_{i}"
            
            # Process videos (movies + TV)
            if target in ("both", "videos"):
                # Movies
                collection = db.dbs[key]["movie"]
                tasks = []
                async for movie in collection.find({}):
                    if _cancel_requested:
                        break
                    tasks.append(process_movie_concurrent(collection, movie))
                    
                    if len(tasks) >= CONCURRENCY * 2:
                        await asyncio.gather(*tasks, return_exceptions=True)
                        _current_operation.checked = checked_count
                        _current_operation.removed = removed_count
                        _current_operation.videos_removed = videos_removed
                        _current_operation.unsorted_removed = unsorted_removed
                        _current_operation.processed_items = processed_count
                        tasks = []
                
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    _current_operation.checked = checked_count
                    _current_operation.removed = removed_count
                    _current_operation.videos_removed = videos_removed
                    _current_operation.unsorted_removed = unsorted_removed
                    _current_operation.processed_items = processed_count
                
                # TV Shows
                collection = db.dbs[key]["tv"]
                tasks = []
                async for tv_show in collection.find({}):
                    if _cancel_requested:
                        break
                    tasks.append(process_tv_concurrent(collection, tv_show))
                    
                    if len(tasks) >= CONCURRENCY * 2:
                        await asyncio.gather(*tasks, return_exceptions=True)
                        _current_operation.checked = checked_count
                        _current_operation.removed = removed_count
                        _current_operation.videos_removed = videos_removed
                        _current_operation.unsorted_removed = unsorted_removed
                        _current_operation.processed_items = processed_count
                        tasks = []
                
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    _current_operation.checked = checked_count
                    _current_operation.removed = removed_count
                    _current_operation.videos_removed = videos_removed
                    _current_operation.unsorted_removed = unsorted_removed
                    _current_operation.processed_items = processed_count
            
            # Process unsorted files
            if target in ("both", "unsorted"):
                collection = unsorted_collection.dbs[key][unsorted_collection.COLLECTION_NAME]
                tasks = []
                async for file_doc in collection.find({}):
                    if _cancel_requested:
                        break
                    tasks.append(process_unsorted_concurrent(collection, file_doc))
                    
                    if len(tasks) >= CONCURRENCY * 2:
                        await asyncio.gather(*tasks, return_exceptions=True)
                        _current_operation.checked = checked_count
                        _current_operation.removed = removed_count
                        _current_operation.videos_removed = videos_removed
                        _current_operation.unsorted_removed = unsorted_removed
                        _current_operation.processed_items = processed_count
                        tasks = []
                
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    _current_operation.checked = checked_count
                    _current_operation.removed = removed_count
                    _current_operation.videos_removed = videos_removed
                    _current_operation.unsorted_removed = unsorted_removed
                    _current_operation.processed_items = processed_count
        
        # Wait for any pending delete tasks to complete
        if _pending_delete_tasks:
            _current_operation.current_step = "Cleaning up..."
            await asyncio.gather(*_pending_delete_tasks, return_exceptions=True)
            _pending_delete_tasks.clear()
        
        _current_operation.end_time = time.time()
        _current_operation.status = "cancelled" if _cancel_requested else "completed"
        _current_operation.current_step = "Cancelled" if _cancel_requested else "Done"
        
    except Exception as e:
        LOGGER.error(f"[Tidy] Error: {e}")
        _current_operation.status = "error"
        _current_operation.error_message = str(e)
        _current_operation.end_time = time.time()
        # Clean up pending tasks on error too
        if _pending_delete_tasks:
            await asyncio.gather(*_pending_delete_tasks, return_exceptions=True)
            _pending_delete_tasks.clear()
    
    return _current_operation


# -------------------------------
# Scan Core Logic
# -------------------------------
SCAN_BUFFER = 1000


def normalize_channel_id(channel_id) -> int:
    """Ensure channel_id is in -100XXXXXXXXXX format for Telegram API"""
    channel_str = str(channel_id).strip()
    if channel_str.startswith("-100"):
        return int(channel_str)
    channel_str = channel_str.lstrip("-")
    return int(f"-100{channel_str}")


def get_raw_channel_id(channel_id) -> str:
    """Get raw channel ID without -100 prefix for DB comparison"""
    channel_str = str(channel_id).strip()
    if channel_str.startswith("-100"):
        return channel_str[4:]
    return channel_str.lstrip("-")


async def build_existing_msg_ids() -> Tuple[Set[Tuple[str, int]], Set[Tuple[str, int]], Set[Tuple[str, int]]]:
    """
    Build sets of (channel_id, msg_id) for all existing entries.
    
    Returns:
        Tuple of (all_existing, video_existing, unsorted_existing)
    """
    video_existing = set()
    unsorted_existing = set()
    
    for i in range(1, db.current_db_index + 1):
        db_key = f"storage_{i}"
        
        async for movie in db.dbs[db_key]["movie"].find({}, {"telegram": 1}):
            for t in movie.get("telegram", []):
                try:
                    decoded = await decode_string(t.get("id", ""))
                    video_existing.add((str(decoded.get("chat_id")), int(decoded.get("msg_id"))))
                except Exception:
                    pass
        
        async for tv in db.dbs[db_key]["tv"].find({}, {"seasons": 1}):
            for season in tv.get("seasons", []):
                for episode in season.get("episodes", []):
                    for t in episode.get("telegram", []):
                        try:
                            decoded = await decode_string(t.get("id", ""))
                            video_existing.add((str(decoded.get("chat_id")), int(decoded.get("msg_id"))))
                        except Exception:
                            pass
        
        # Include unsorted files
        async for unsorted in unsorted_collection.dbs[db_key][unsorted_collection.COLLECTION_NAME].find({}, {"telegram_id": 1}):
            try:
                telegram_id = unsorted.get("telegram_id")
                if telegram_id:
                    decoded = await decode_string(telegram_id)
                    unsorted_existing.add((str(decoded.get("chat_id")), int(decoded.get("msg_id"))))
            except Exception:
                pass
    
    all_existing = video_existing | unsorted_existing
    return all_existing, video_existing, unsorted_existing


async def process_unsorted_scan_message(bot, message, stats: dict, progress: OperationProgress, error: str = None) -> bool:
    """Process a non-video message and add to unsorted collection."""
    try:
        from Backend.pyrofork.plugins.unsorted_receiver import handle_unsorted_file
        
        result = await handle_unsorted_file(bot, message, error=error)
        
        if result:
            stats["unsorted_added"] += 1
            progress.unsorted_added += 1
            LOGGER.info(f"[Scan] Added to unsorted: message {message.id}")
            return True
        else:
            stats["skipped_unsorted"] += 1
            return False
            
    except Exception as e:
        LOGGER.error(f"[Scan] Error processing unsorted message {message.id}: {e}")
        stats["errors"] += 1
        progress.errors += 1
        return False


async def process_scan_message(bot, message, stats: dict, progress: OperationProgress) -> bool:
    """
    Process a single message for scan operation.
    Videos go to movies/TV, non-videos go to unsorted.
    """
    try:
        # Check if it's a video
        is_video = message.video or (message.document and message.document.mime_type and 
                message.document.mime_type.startswith("video/"))
        
        if is_video:
            # Process as video (movie/TV)
            file = message.video or message.document
            title = message.caption or file.file_name
            msg_id = message.id
            size = get_readable_file_size(file.file_size)
            channel = str(message.chat.id).replace("-100", "")
            
            metadata_info = await metadata(clean_filename(title), int(channel), msg_id)
            if metadata_info is None:
                LOGGER.warning(f"[Scan] Metadata failed for video: {title}")
                stats["failed_metadata"] += 1
                # Fall back to unsorted
                return await process_unsorted_scan_message(bot, message, stats, progress, error="Metadata lookup failed")
            
            metadata_info['file_size_bytes'] = file.file_size
            metadata_info['telegram_date'] = to_utc_isoformat(message.date)
            
            title = remove_urls(title)
            if not title.endswith(('.mkv', '.mp4')):
                title += '.mkv'
            
            updated_id = await db.insert_media(metadata_info, channel=int(channel), msg_id=msg_id, size=size, name=title)
            
            if updated_id:
                if metadata_info.get('media_type') == 'movie':
                    stats["movies_added"] += 1
                    progress.movies_added += 1
                else:
                    stats["tv_added"] += 1
                    progress.tv_added += 1
                LOGGER.info(f"[Scan] Added: {metadata_info.get('title')}")
                return True
            else:
                stats["failed_insert"] += 1
                return False
        
        # Non-video file - add to unsorted
        elif message.document or message.audio or message.voice or message.video_note or message.animation:
            return await process_unsorted_scan_message(bot, message, stats, progress)
        
        else:
            # No file attachment
            return False
            
    except Exception as e:
        LOGGER.error(f"[Scan] Error processing message {message.id}: {e}")
        stats["errors"] += 1
        progress.errors += 1
        return False


async def run_scan(
    bot,
    channels: list,
    mode: str = "date",  # "date" or "count"
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    limit: Optional[int] = None,
    mode_label: Optional[str] = None
) -> OperationProgress:
    """
    Run the scan operation using the provided bot client.
    
    Args:
        bot: Pyrogram Client instance
        channels: List of channel IDs to scan
        mode: "date" for date range, "count" for message count
        start_date: Start of date range (for date mode)
        end_date: End of date range (for date mode)
        limit: Number of messages to scan (for count mode)
        mode_label: Display label for the mode (e.g., "Last 24 hours")
    """
    global _current_operation, _cancel_requested
    
    async with _operation_lock:
        if is_operation_running():
            raise RuntimeError("Another operation is already running")
        
        _cancel_requested = False
        _current_operation = OperationProgress(operation_type="scan")
        _current_operation.status = "running"
        _current_operation.start_time = time.time()
        _current_operation.current_step = "Building index..."
    
    # Use provided mode_label or calculate a default
    if mode_label:
        _current_operation.mode_label = mode_label
    elif mode == "date":
        if start_date and end_date:
            hours_diff = (end_date - start_date).total_seconds() / 3600
            if hours_diff <= 24:
                _current_operation.mode_label = "Last 24 hours"
            else:
                days = int(hours_diff / 24)
                _current_operation.mode_label = f"Last {days} days"
        elif start_date:
            _current_operation.mode_label = f"Since {start_date.strftime('%Y-%m-%d')}"
        else:
            _current_operation.mode_label = "All time"
    else:
        _current_operation.mode_label = f"Last {limit or 'all'} messages"
    
    # Set total_items for progress tracking
    if mode == "date":
        _current_operation.total_items = 0  # Unknown, will use processed_items
    else:
        _current_operation.total_items = limit or 0
    
    stats = {
        "messages_checked": 0,
        "movies_added": 0,
        "tv_added": 0,
        "unsorted_added": 0,
        "already_processed": 0,
        "already_processed_videos": 0,
        "already_processed_unsorted": 0,
        "skipped_no_file": 0,
        "skipped_unsorted": 0,
        "failed_metadata": 0,
        "failed_insert": 0,
        "errors": 0
    }
    
    RATE_LIMIT_DELAY = 0.3
    # Optimize batch size based on limit (smaller batches for small counts)
    if mode == "count" and limit and limit < 100:
        BATCH_SIZE = min(50, limit * 2)  # Smaller batches for small limits
    else:
        BATCH_SIZE = 200
    PROGRESS_UPDATE_INTERVAL = 10  # Update progress every N messages for responsive UI
    
    try:
        # Build existing message IDs (returns all, videos, unsorted sets)
        existing_msg_ids, video_msg_ids, unsorted_msg_ids = await build_existing_msg_ids()
        LOGGER.info(f"[Scan] Found {len(existing_msg_ids)} existing entries (Videos: {len(video_msg_ids)}, Unsorted: {len(unsorted_msg_ids)})")
        
        if _cancel_requested:
            _current_operation.status = "cancelled"
            _current_operation.end_time = time.time()
            return _current_operation
        
        _current_operation.current_step = "Scanning channels..."
        
        for channel_id in channels:
            if _cancel_requested:
                break
            
            chat_id = normalize_channel_id(channel_id)
            raw_channel_id = get_raw_channel_id(channel_id)
            
            try:
                chat = await bot.get_chat(chat_id)
                _current_operation.channel_name = chat.title or str(channel_id)
            except Exception:
                _current_operation.channel_name = str(channel_id)
            
            # Find max known msg_id for this channel
            max_known_msg_id = 0
            for (ch, msg_id) in existing_msg_ids:
                if ch == raw_channel_id and msg_id > max_known_msg_id:
                    max_known_msg_id = msg_id
            
            upper_bound = max_known_msg_id + SCAN_BUFFER
            lower_bound = 1
            current_id = upper_bound
            messages_scanned = 0
            scan_complete = False
            
            while current_id >= lower_bound and not scan_complete:
                if _cancel_requested:
                    break
                
                # Count mode: stop if we've processed enough messages
                if mode == "count" and limit and stats["messages_checked"] >= limit:
                    break
                
                batch_end = current_id
                batch_start = max(lower_bound, current_id - BATCH_SIZE + 1)
                msg_ids = list(range(batch_start, batch_end + 1))
                
                if not msg_ids:
                    break
                
                try:
                    messages = await bot.get_messages(chat_id, msg_ids)
                    if not isinstance(messages, list):
                        messages = [messages] if messages else []
                except FloodWait as e:
                    LOGGER.warning(f"[Scan] FloodWait: sleeping {e.value}s")
                    await asyncio.sleep(e.value)
                    continue  # Retry same batch
                except Exception as e:
                    LOGGER.debug(f"[Scan] Error fetching batch: {e}")
                    current_id = batch_start - 1
                    continue
                
                # CRITICAL FIX: Sort messages by ID descending (newest first)
                # This ensures date filtering works correctly - we process newest messages
                # first, so when we hit an old message outside the date range, we've already
                # processed all newer messages in the batch.
                # Also filters out empty/deleted messages upfront to avoid repeated checks.
                messages = sorted(
                    [m for m in messages if m and not m.empty],
                    key=lambda m: m.id,
                    reverse=True
                )
                
                for message in messages:
                    if _cancel_requested:
                        break
                    
                    messages_scanned += 1
                    
                    # Date mode: check if message is within date range
                    # Since we're iterating newest-to-oldest, once we hit an old message,
                    # all remaining messages in this batch are also old - so we can break.
                    if mode == "date":
                        msg_date = message.date.replace(tzinfo=None) if message.date else None
                        if start_date and msg_date and msg_date < start_date:
                            scan_complete = True
                            break
                        if end_date and msg_date and msg_date > end_date:
                            continue
                    
                    # Count messages that pass filters
                    stats["messages_checked"] += 1
                    
                    # Classify message and process appropriately
                    msg_key = (raw_channel_id, message.id)
                    if msg_key in existing_msg_ids:
                        stats["already_processed"] += 1
                        # Track breakdown: was it a video or unsorted?
                        if msg_key in video_msg_ids:
                            stats["already_processed_videos"] += 1
                        elif msg_key in unsorted_msg_ids:
                            stats["already_processed_unsorted"] += 1
                    elif message.video or (message.document and message.document.mime_type and 
                            message.document.mime_type.startswith("video/")):
                        # Video file -> try movie/TV, fallback to unsorted
                        await process_scan_message(bot, message, stats, _current_operation)
                        await asyncio.sleep(RATE_LIMIT_DELAY)
                    elif message.document or message.audio or message.voice or message.video_note or message.animation:
                        # Non-video file -> unsorted collection
                        await process_scan_message(bot, message, stats, _current_operation)
                        await asyncio.sleep(RATE_LIMIT_DELAY)
                    else:
                        # No file attachment (text only, stickers, etc.)
                        stats["skipped_no_file"] += 1
                    
                    # Count mode: stop AFTER classifying the message
                    if mode == "count" and limit and stats["messages_checked"] >= limit:
                        scan_complete = True
                        break
                    
                    # Progress updates every few messages
                    if stats["messages_checked"] % PROGRESS_UPDATE_INTERVAL == 0:
                        _current_operation.processed_items = messages_scanned
                        _current_operation.checked = stats["messages_checked"]
                        _current_operation.already_processed = stats["already_processed"]
                        _current_operation.already_processed_videos = stats["already_processed_videos"]
                        _current_operation.already_processed_unsorted = stats["already_processed_unsorted"]
                        _current_operation.skipped_no_file = stats["skipped_no_file"]
                        _current_operation.unsorted_added = stats["unsorted_added"]
                        _current_operation.failed_metadata = stats["failed_metadata"]
                
                # Update progress at end of each batch
                _current_operation.processed_items = messages_scanned
                _current_operation.checked = stats["messages_checked"]
                _current_operation.already_processed = stats["already_processed"]
                _current_operation.already_processed_videos = stats["already_processed_videos"]
                _current_operation.already_processed_unsorted = stats["already_processed_unsorted"]
                _current_operation.skipped_no_file = stats["skipped_no_file"]
                _current_operation.unsorted_added = stats["unsorted_added"]
                _current_operation.failed_metadata = stats["failed_metadata"]
                
                current_id = batch_start - 1
                await asyncio.sleep(0.05)
        
        # Final sync of all stats
        _current_operation.processed_items = messages_scanned
        _current_operation.checked = stats["messages_checked"]
        _current_operation.already_processed = stats["already_processed"]
        _current_operation.already_processed_videos = stats["already_processed_videos"]
        _current_operation.already_processed_unsorted = stats["already_processed_unsorted"]
        _current_operation.skipped_no_file = stats["skipped_no_file"]
        _current_operation.failed_metadata = stats["failed_metadata"]
        _current_operation.movies_added = stats["movies_added"]
        _current_operation.tv_added = stats["tv_added"]
        _current_operation.unsorted_added = stats["unsorted_added"]
        _current_operation.errors = stats["errors"]
        
        _current_operation.end_time = time.time()
        _current_operation.status = "cancelled" if _cancel_requested else "completed"
        _current_operation.current_step = "Cancelled" if _cancel_requested else "Done"
        
    except Exception as e:
        LOGGER.error(f"[Scan] Error: {e}")
        _current_operation.status = "error"
        _current_operation.error_message = str(e)
        _current_operation.end_time = time.time()
    
    return _current_operation


def get_auth_channels() -> list:
    """Get list of configured AUTH_CHANNELs"""
    channels = Telegram.AUTH_CHANNEL
    if isinstance(channels, (list, tuple)):
        return list(channels)
    elif channels:
        return [channels]
    return []


async def reset_operation():
    """Reset operation state (async to properly acquire lock)"""
    global _current_operation, _cancel_requested, _pending_delete_tasks
    
    async with _operation_lock:
        # Wait for any pending delete tasks to complete
        if _pending_delete_tasks:
            await asyncio.gather(*_pending_delete_tasks, return_exceptions=True)
        
        _current_operation = None
        _cancel_requested = False
        _pending_delete_tasks = []

