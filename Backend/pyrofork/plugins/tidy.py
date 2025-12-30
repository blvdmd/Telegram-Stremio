"""
/tidy command - Cleanup orphaned/invalid telegram entries from database.

This command:
1. Scans all movies and TV shows in the database
2. Checks if each telegram entry's original message still exists
3. Removes entries whose messages are deleted or don't contain valid media
4. Attempts to delete orphaned messages from the Telegram channel
"""

import time
import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from Backend import db
from Backend.helper.custom_filter import CustomFilters
from Backend.helper.encrypt import decode_string
from Backend.helper.task_manager import delete_message
from Backend.logger import LOGGER

TIDY_CANCEL_REQUESTED = False


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
# CANCEL BUTTON HANDLER
# -------------------------------
@Client.on_callback_query(filters.regex("cancel_tidy"))
async def cancel_tidy(_, query):
    global TIDY_CANCEL_REQUESTED
    TIDY_CANCEL_REQUESTED = True
    await query.message.edit_text("❌ Tidy operation has been cancelled by the user.")
    await query.answer("Cancelled")


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
                try:
                    asyncio.create_task(delete_message(chat_id, msg_id))
                except Exception as e:
                    LOGGER.debug(f"Could not delete message {msg_id}: {e}")
    
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
                        try:
                            asyncio.create_task(delete_message(chat_id, msg_id))
                        except Exception as e:
                            LOGGER.debug(f"Could not delete message {msg_id}: {e}")
            
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
# MAIN COMMAND
# -------------------------------
@Client.on_message(filters.command("tidy") & filters.private & CustomFilters.owner, group=11)
async def tidy_handler(client, message):
    global TIDY_CANCEL_REQUESTED
    TIDY_CANCEL_REQUESTED = False

    # -------------------------
    # Gather totals
    # -------------------------
    total_movies = 0
    total_tv = 0
    for i in range(1, db.current_db_index + 1):
        key = f"storage_{i}"
        total_movies += await db.dbs[key]["movie"].count_documents({})
        total_tv += await db.dbs[key]["tv"].count_documents({})

    TOTAL = total_movies + total_tv
    DONE = 0
    start_time = time.time()
    
    total_checked = 0
    total_removed = 0

    status = await message.reply_text(
        "🧹 Initializing tidy operation...\n\n"
        "This will scan all entries and remove those\n"
        "whose Telegram messages no longer exist.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_tidy")]
        ])
    )

    # -------------------------
    # Tunables
    # -------------------------
    CONCURRENCY = 5  # Lower than fixmetadata due to message checks
    PROGRESS_INTERVAL = 3.0
    RATE_LIMIT_DELAY = 0.5  # Delay between API calls

    semaphore = asyncio.Semaphore(CONCURRENCY)
    last_progress_edit = start_time

    async def process_movie_with_semaphore(collection, movie):
        nonlocal DONE, total_checked, total_removed, last_progress_edit
        
        if TIDY_CANCEL_REQUESTED:
            return
        
        async with semaphore:
            stats = await tidy_movie(client, collection, movie)
            total_checked += stats["checked"]
            total_removed += stats["removed"]
            DONE += 1
            await asyncio.sleep(RATE_LIMIT_DELAY)
        
        # Update progress
        now = time.time()
        if now - last_progress_edit > PROGRESS_INTERVAL:
            last_progress_edit = now
            try:
                await status.edit_text(
                    f"🧹 Tidying database...\n"
                    f"{progress_bar(DONE, TOTAL)}\n"
                    f"📊 Checked: {total_checked} | Removed: {total_removed}\n"
                    f"⏱ Elapsed: {format_eta(now - start_time)}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_tidy")]
                    ])
                )
            except Exception:
                pass

    async def process_tv_with_semaphore(collection, tv_show):
        nonlocal DONE, total_checked, total_removed, last_progress_edit
        
        if TIDY_CANCEL_REQUESTED:
            return
        
        async with semaphore:
            stats = await tidy_tv_show(client, collection, tv_show)
            total_checked += stats["checked"]
            total_removed += stats["removed"]
            DONE += 1
            await asyncio.sleep(RATE_LIMIT_DELAY)
        
        # Update progress
        now = time.time()
        if now - last_progress_edit > PROGRESS_INTERVAL:
            last_progress_edit = now
            try:
                await status.edit_text(
                    f"🧹 Tidying database...\n"
                    f"{progress_bar(DONE, TOTAL)}\n"
                    f"📊 Checked: {total_checked} | Removed: {total_removed}\n"
                    f"⏱ Elapsed: {format_eta(now - start_time)}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_tidy")]
                    ])
                )
            except Exception:
                pass

    # -------------------------
    # Process all databases
    # -------------------------
    try:
        for i in range(1, db.current_db_index + 1):
            if TIDY_CANCEL_REQUESTED:
                break
                
            db_key = f"storage_{i}"
            LOGGER.info(f"[Tidy] Processing {db_key}")
            
            # Process movies
            movie_collection = db.dbs[db_key]["movie"]
            movie_cursor = movie_collection.find({})
            
            tasks = []
            async for movie in movie_cursor:
                if TIDY_CANCEL_REQUESTED:
                    break
                tasks.append(process_movie_with_semaphore(movie_collection, movie))
                
                # Process in batches
                if len(tasks) >= CONCURRENCY * 2:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process TV shows
            tv_collection = db.dbs[db_key]["tv"]
            tv_cursor = tv_collection.find({})
            
            tasks = []
            async for tv_show in tv_cursor:
                if TIDY_CANCEL_REQUESTED:
                    break
                tasks.append(process_tv_with_semaphore(tv_collection, tv_show))
                
                # Process in batches
                if len(tasks) >= CONCURRENCY * 2:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        LOGGER.exception(f"[Tidy] Error during tidy operation: {e}")

    # -------------------------
    # Final status
    # -------------------------
    if TIDY_CANCEL_REQUESTED:
        try:
            await status.edit_text(
                f"❌ Tidy operation cancelled.\n\n"
                f"📊 Progress before cancellation:\n"
                f"• Entries checked: {total_checked}\n"
                f"• Entries removed: {total_removed}\n"
                f"• Time elapsed: {format_eta(time.time() - start_time)}"
            )
        except Exception:
            pass
        return

    elapsed = time.time() - start_time
    try:
        await status.edit_text(
            f"✅ **Tidy Complete!**\n\n"
            f"📊 **Summary:**\n"
            f"• Media items scanned: {TOTAL}\n"
            f"• Telegram entries checked: {total_checked}\n"
            f"• Invalid entries removed: {total_removed}\n"
            f"• Time taken: {format_eta(elapsed)}"
        )
    except Exception:
        pass
    
    LOGGER.info(f"[Tidy] Completed - Checked: {total_checked}, Removed: {total_removed}")

