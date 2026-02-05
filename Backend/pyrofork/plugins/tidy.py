"""
/tidy command - Cleanup orphaned/invalid telegram entries from database.

This command:
1. Shows user 3 options: Both, Videos Only, Unsorted Files Only
2. Scans selected collections in the database
3. Checks if each telegram entry's original message still exists
4. Removes entries whose messages are deleted or don't contain valid media
5. Attempts to delete orphaned messages from the Telegram channel
"""

import time
import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from Backend import db, unsorted_collection
from Backend.helper.custom_filter import CustomFilters
from Backend.helper.encrypt import decode_string
from Backend.helper.task_manager import delete_message
from Backend.logger import LOGGER

TIDY_CANCEL_REQUESTED = False
TIDY_STATE = {}  # Store user state for multi-step flow
_pending_delete_tasks = []  # Track delete tasks to prevent GC


# -------------------------------
# Progress Bar Helper
# -------------------------------
def progress_bar(done, total, length=20):
    filled = int(length * (done / total)) if total else length
    return f"[{'█' * filled}{'░' * (length - filled)}] {done}/{total}"


# -------------------------------
# ETA Helper
# -------------------------------
def format_eta(seconds):
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {sec}s"
    if minutes > 0:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


# -------------------------------
# CALLBACK HANDLERS
# -------------------------------
@Client.on_callback_query(filters.regex("cancel_tidy"))
async def cancel_tidy(_, query: CallbackQuery):
    global TIDY_CANCEL_REQUESTED
    TIDY_CANCEL_REQUESTED = True
    user_id = query.from_user.id
    if user_id in TIDY_STATE:
        del TIDY_STATE[user_id]
    await query.message.edit_text("❌ Tidy operation has been cancelled by the user.")
    await query.answer("Cancelled")


@Client.on_callback_query(filters.regex(r"^tidy_target_(both|videos|unsorted)$"))
async def select_tidy_target(client: Client, query: CallbackQuery):
    """Handle tidy target selection"""
    global TIDY_CANCEL_REQUESTED
    TIDY_CANCEL_REQUESTED = False
    
    user_id = query.from_user.id
    target = query.data.split("_")[2]  # "both", "videos", or "unsorted"
    
    TIDY_STATE[user_id] = {"target": target}
    await query.answer()
    
    # Start the tidy operation
    await run_tidy_operation(client, query.message, user_id, target)


# -------------------------------
# Check if telegram message is valid
# -------------------------------
async def is_message_valid(bot: Client, telegram_id: str) -> tuple[bool, int, int]:
    """
    Check if a telegram message exists and contains valid media.
    
    Returns:
        tuple: (is_valid, chat_id, msg_id)
    """
    try:
        decoded = await decode_string(telegram_id)
        chat_id = int(f"-100{decoded['chat_id']}")
        msg_id = int(decoded['msg_id'])
        
        message = await bot.get_messages(chat_id, msg_id)
        
        # Check if message exists and has video or document
        if message and (message.video or message.document):
            return (True, chat_id, msg_id)
        else:
            return (False, chat_id, msg_id)
            
    except Exception as e:
        LOGGER.debug(f"Error checking message validity: {e}")
        return (False, 0, 0)


# -------------------------------
# Process single movie
# -------------------------------
def _track_delete_task(chat_id: int, msg_id: int):
    """Create and track a delete message task to prevent GC."""
    global _pending_delete_tasks
    try:
        task = asyncio.create_task(delete_message(chat_id, msg_id))
        _pending_delete_tasks.append(task)
    except Exception as e:
        LOGGER.debug(f"Could not create delete task for {msg_id}: {e}")


async def tidy_movie(bot: Client, collection, movie: dict) -> dict:
    """
    Check and clean up invalid telegram entries for a movie.
    
    Returns:
        dict with counts: {"checked": int, "removed": int}
    """
    stats = {"checked": 0, "removed": 0}
    tmdb_id = movie.get("tmdb_id")
    telegram_list = movie.get("telegram", [])
    
    if not telegram_list:
        return stats
    
    valid_entries = []
    
    for telegram_item in telegram_list:
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
            
            # Try to delete from Telegram if we have valid IDs
            if chat_id and msg_id:
                _track_delete_task(chat_id, msg_id)
    
    # Update database if entries were removed
    if stats["removed"] > 0:
        try:
            if valid_entries:
                # Update with remaining valid entries
                await collection.update_one(
                    {"tmdb_id": tmdb_id},
                    {"$set": {"telegram": valid_entries}}
                )
            else:
                # No valid entries left - delete the entire movie
                await collection.delete_one({"tmdb_id": tmdb_id})
                LOGGER.info(f"[Tidy][Movie] Deleted movie with no valid entries: {movie.get('title')}")
        except Exception as e:
            LOGGER.error(f"[Tidy][Movie] DB update failed for {movie.get('title')}: {e}")
    
    return stats


# -------------------------------
# Process single TV show
# -------------------------------
async def tidy_tv_show(bot: Client, collection, tv_show: dict) -> dict:
    """
    Check and clean up invalid telegram entries for a TV show.
    
    Returns:
        dict with counts: {"checked": int, "removed": int}
    """
    stats = {"checked": 0, "removed": 0}
    tmdb_id = tv_show.get("tmdb_id")
    seasons = tv_show.get("seasons", [])
    modified = False
    
    for season in seasons:
        episodes_to_keep = []
        
        for episode in season.get("episodes", []):
            telegram_list = episode.get("telegram", [])
            valid_entries = []
            
            for telegram_item in telegram_list:
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
                        f"S{season.get('season_number')}E{episode.get('episode_number')} - {telegram_item.get('quality')}"
                    )
                    
                    # Try to delete from Telegram if we have valid IDs
                    if chat_id and msg_id:
                        _track_delete_task(chat_id, msg_id)
            
            # Keep episode if it has valid entries
            if valid_entries:
                episode["telegram"] = valid_entries
                episodes_to_keep.append(episode)
        
        # Update season's episodes
        season["episodes"] = episodes_to_keep
    
    # Remove empty seasons
    seasons = [s for s in seasons if s.get("episodes")]
    
    # Update database if modified
    if modified:
        try:
            if seasons:
                # Update with remaining valid data
                await collection.update_one(
                    {"tmdb_id": tmdb_id},
                    {"$set": {"seasons": seasons}}
                )
            else:
                # No valid entries left - delete the entire TV show
                await collection.delete_one({"tmdb_id": tmdb_id})
                LOGGER.info(f"[Tidy][TV] Deleted TV show with no valid entries: {tv_show.get('title')}")
        except Exception as e:
            LOGGER.error(f"[Tidy][TV] DB update failed for {tv_show.get('title')}: {e}")
    
    return stats


# -------------------------------
# Unsorted File Helpers
# -------------------------------
async def is_unsorted_message_valid(bot: Client, telegram_id: str) -> tuple[bool, int, int]:
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


async def tidy_unsorted_file(bot: Client, collection, file_doc: dict) -> dict:
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


# -------------------------------
# MAIN TIDY OPERATION
# -------------------------------
async def run_tidy_operation(client: Client, status_message, user_id: int, target: str):
    """
    Run the tidy operation for the selected target.
    
    Args:
        client: Pyrogram client
        status_message: Message to update with progress
        user_id: User ID
        target: "both", "videos", or "unsorted"
    """
    global TIDY_CANCEL_REQUESTED
    
    target_labels = {
        "both": "Videos + Unsorted Files",
        "videos": "Videos Only",
        "unsorted": "Unsorted Files Only"
    }
    
    # -------------------------
    # Gather totals based on target
    # -------------------------
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

    TOTAL = total_movies + total_tv + total_unsorted
    DONE = 0
    start_time = time.time()
    
    total_checked = 0
    total_removed = 0
    videos_removed = 0
    unsorted_removed = 0

    await status_message.edit_text(
        f"🧹 **Tidying: {target_labels[target]}**\n\n"
        f"{progress_bar(0, TOTAL)}\n"
        f"📊 Checked: 0 | Removed: 0\n"
        f"⏱ Starting...",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_tidy")]
        ])
    )

    # -------------------------
    # Tunables
    # -------------------------
    CONCURRENCY = 5
    PROGRESS_INTERVAL = 3.0
    RATE_LIMIT_DELAY = 0.5

    semaphore = asyncio.Semaphore(CONCURRENCY)
    last_progress_edit = start_time

    async def update_progress():
        nonlocal last_progress_edit
        now = time.time()
        if now - last_progress_edit > PROGRESS_INTERVAL:
            last_progress_edit = now
            try:
                await status_message.edit_text(
                    f"🧹 **Tidying: {target_labels[target]}**\n\n"
                    f"{progress_bar(DONE, TOTAL)}\n"
                    f"📊 Checked: {total_checked} | Removed: {total_removed}\n"
                    f"⏱ Elapsed: {format_eta(now - start_time)}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_tidy")]
                    ])
                )
            except Exception:
                pass

    async def process_movie_with_semaphore(collection, movie):
        nonlocal DONE, total_checked, total_removed, videos_removed
        if TIDY_CANCEL_REQUESTED:
            return
        async with semaphore:
            stats = await tidy_movie(client, collection, movie)
            total_checked += stats["checked"]
            total_removed += stats["removed"]
            videos_removed += stats["removed"]
            DONE += 1
            await asyncio.sleep(RATE_LIMIT_DELAY)
        await update_progress()

    async def process_tv_with_semaphore(collection, tv_show):
        nonlocal DONE, total_checked, total_removed, videos_removed
        if TIDY_CANCEL_REQUESTED:
            return
        async with semaphore:
            stats = await tidy_tv_show(client, collection, tv_show)
            total_checked += stats["checked"]
            total_removed += stats["removed"]
            videos_removed += stats["removed"]
            DONE += 1
            await asyncio.sleep(RATE_LIMIT_DELAY)
        await update_progress()

    async def process_unsorted_with_semaphore(collection, file_doc):
        nonlocal DONE, total_checked, total_removed, unsorted_removed
        if TIDY_CANCEL_REQUESTED:
            return
        async with semaphore:
            stats = await tidy_unsorted_file(client, collection, file_doc)
            total_checked += stats["checked"]
            total_removed += stats["removed"]
            unsorted_removed += stats["removed"]
            DONE += 1
            await asyncio.sleep(RATE_LIMIT_DELAY)
        await update_progress()

    # -------------------------
    # Process databases
    # -------------------------
    try:
        for i in range(1, db.current_db_index + 1):
            if TIDY_CANCEL_REQUESTED:
                break
                
            db_key = f"storage_{i}"
            LOGGER.info(f"[Tidy] Processing {db_key} (target: {target})")
            
            # Process videos (movies + TV)
            if target in ("both", "videos"):
                # Movies
                movie_collection = db.dbs[db_key]["movie"]
                tasks = []
                async for movie in movie_collection.find({}):
                    if TIDY_CANCEL_REQUESTED:
                        break
                    tasks.append(process_movie_with_semaphore(movie_collection, movie))
                    if len(tasks) >= CONCURRENCY * 2:
                        await asyncio.gather(*tasks, return_exceptions=True)
                        tasks = []
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                
                # TV Shows
                tv_collection = db.dbs[db_key]["tv"]
                tasks = []
                async for tv_show in tv_collection.find({}):
                    if TIDY_CANCEL_REQUESTED:
                        break
                    tasks.append(process_tv_with_semaphore(tv_collection, tv_show))
                    if len(tasks) >= CONCURRENCY * 2:
                        await asyncio.gather(*tasks, return_exceptions=True)
                        tasks = []
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process unsorted files
            if target in ("both", "unsorted"):
                unsorted_coll = unsorted_collection.dbs[db_key][unsorted_collection.COLLECTION_NAME]
                tasks = []
                async for file_doc in unsorted_coll.find({}):
                    if TIDY_CANCEL_REQUESTED:
                        break
                    tasks.append(process_unsorted_with_semaphore(unsorted_coll, file_doc))
                    if len(tasks) >= CONCURRENCY * 2:
                        await asyncio.gather(*tasks, return_exceptions=True)
                        tasks = []
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        LOGGER.exception(f"[Tidy] Error during tidy operation: {e}")

    # -------------------------
    # Wait for pending delete tasks
    # -------------------------
    global _pending_delete_tasks
    if _pending_delete_tasks:
        try:
            await status_message.edit_text(
                f"🧹 **Tidying: {target_labels[target]}**\n\n"
                f"📊 Cleaning up... ({len(_pending_delete_tasks)} pending deletes)"
            )
        except Exception:
            pass
        await asyncio.gather(*_pending_delete_tasks, return_exceptions=True)
        _pending_delete_tasks = []

    # -------------------------
    # Final status
    # -------------------------
    # Cleanup state
    if user_id in TIDY_STATE:
        del TIDY_STATE[user_id]
    
    if TIDY_CANCEL_REQUESTED:
        try:
            await status_message.edit_text(
                f"❌ **Tidy Cancelled**\n\n"
                f"📊 **Progress before cancellation:**\n"
                f"• Entries checked: {total_checked}\n"
                f"• Entries removed: {total_removed}\n"
                f"• Time elapsed: {format_eta(time.time() - start_time)}"
            )
        except Exception:
            pass
        return

    elapsed = time.time() - start_time
    
    # Build summary based on target
    summary_lines = [f"• Total items scanned: {TOTAL}", f"• Telegram entries checked: {total_checked}"]
    
    if target == "both":
        summary_lines.append(f"• Videos removed: {videos_removed}")
        summary_lines.append(f"• Unsorted files removed: {unsorted_removed}")
    elif target == "videos":
        summary_lines.append(f"• Invalid entries removed: {videos_removed}")
    else:  # unsorted
        summary_lines.append(f"• Orphaned files removed: {unsorted_removed}")
    
    summary_lines.append(f"• Time taken: {format_eta(elapsed)}")
    
    try:
        await status_message.edit_text(
            f"✅ **Tidy Complete!**\n\n"
            f"🎯 **Target:** {target_labels[target]}\n\n"
            f"📊 **Summary:**\n" + "\n".join(summary_lines)
        )
    except Exception:
        pass
    
    LOGGER.info(f"[Tidy] Completed (target: {target}) - Checked: {total_checked}, Removed: {total_removed}")


# -------------------------------
# MAIN COMMAND HANDLER
# -------------------------------
@Client.on_message(filters.command("tidy") & filters.private & CustomFilters.owner, group=11)
async def tidy_handler(client: Client, message):
    """Entry point for /tidy command - shows target selection buttons"""
    global TIDY_CANCEL_REQUESTED
    TIDY_CANCEL_REQUESTED = False
    
    user_id = message.from_user.id
    
    # Clear any previous state
    if user_id in TIDY_STATE:
        del TIDY_STATE[user_id]
    
    await message.reply_text(
        "🧹 **Tidy Database**\n\n"
        "This will scan entries and remove those whose\n"
        "Telegram messages no longer exist.\n\n"
        "**What would you like to tidy?**",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📦 Both", callback_data="tidy_target_both")],
            [InlineKeyboardButton("🎬 Videos Only", callback_data="tidy_target_videos")],
            [InlineKeyboardButton("📁 Unsorted Files Only", callback_data="tidy_target_unsorted")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_tidy")]
        ])
    )

