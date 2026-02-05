"""
/scan_files command - Scan authorized channels for unprocessed messages.

This command:
1. Lets user select channel(s) to scan (if multiple AUTH_CHANNELs)
2. Lets user choose scan mode: by date range OR by message count
3. Processes missing files: videos -> movies/TV, non-videos -> unsorted
4. Shows progress with cancel option and detailed final report
"""

import time
import asyncio
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from pyrogram.errors import FloodWait

from Backend import db, unsorted_collection
from Backend.config import Telegram
from Backend.helper.custom_filter import CustomFilters
from Backend.helper.encrypt import decode_string
from Backend.helper.pyro import clean_filename, get_readable_file_size, remove_urls, to_utc_isoformat
from Backend.helper.metadata import metadata
from Backend.logger import LOGGER


# -------------------------------
# Configuration
# -------------------------------
SCAN_BUFFER = 1000  # Buffer for finding new messages beyond max known ID (handles gaps from deletions)

# -------------------------------
# Global State
# -------------------------------
SCAN_CANCEL_REQUESTED = False
SCAN_STATE = {}  # Store user state for multi-step flow


# -------------------------------
# Helper Functions
# -------------------------------
def progress_bar(done: int, total: int, length: int = 20) -> str:
    if total == 0:
        return f"[{'█' * length}] {done}/{total}"
    filled = int(length * (done / total))
    return f"[{'█' * filled}{'░' * (length - filled)}] {done}/{total}"


def format_eta(seconds: float) -> str:
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {sec}s"
    if minutes > 0:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def format_number(n: int) -> str:
    """Format number with commas for readability"""
    return f"{n:,}"


def normalize_channel_id(channel_id: str) -> int:
    """Convert channel ID string to proper Telegram chat ID format"""
    channel_id = str(channel_id).strip()
    # If already has -100 prefix, just convert to int
    if channel_id.startswith("-100"):
        return int(channel_id)
    # If negative but not -100 prefix, add it
    if channel_id.startswith("-"):
        return int(f"-100{channel_id[1:]}")
    # If positive number, add -100 prefix
    return int(f"-100{channel_id}")


def get_raw_channel_id(channel_id: str) -> str:
    """Get the raw channel ID without -100 prefix (for comparison with DB)"""
    channel_id = str(channel_id).strip()
    if channel_id.startswith("-100"):
        return channel_id[4:]  # Remove -100
    if channel_id.startswith("-"):
        return channel_id[1:]  # Remove just -
    return channel_id


async def get_channel_title(bot: Client, channel_id: str) -> str:
    """Get channel title/username for display"""
    try:
        chat_id = normalize_channel_id(channel_id)
        chat = await bot.get_chat(chat_id)
        return f"@{chat.username}" if chat.username else chat.title or f"Channel {channel_id}"
    except Exception:
        return f"Channel {channel_id}"


async def build_existing_msg_ids() -> tuple:
    """
    Build sets of all message IDs already in database (movies, TV, and unsorted)
    
    Returns:
        Tuple of (all_existing, video_existing, unsorted_existing)
    """
    video_existing = set()
    unsorted_existing = set()
    
    for i in range(1, db.current_db_index + 1):
        db_key = f"storage_{i}"
        
        # Scan movies
        async for movie in db.dbs[db_key]["movie"].find({}, {"telegram": 1}):
            for t in movie.get("telegram", []):
                try:
                    decoded = await decode_string(t.get("id", ""))
                    video_existing.add((str(decoded.get("chat_id")), int(decoded.get("msg_id"))))
                except Exception:
                    pass
        
        # Scan TV shows
        async for tv in db.dbs[db_key]["tv"].find({}, {"seasons": 1}):
            for season in tv.get("seasons", []):
                for episode in season.get("episodes", []):
                    for t in episode.get("telegram", []):
                        try:
                            decoded = await decode_string(t.get("id", ""))
                            video_existing.add((str(decoded.get("chat_id")), int(decoded.get("msg_id"))))
                        except Exception:
                            pass
        
        # Scan unsorted files
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


async def process_video_message(bot: Client, message: Message, stats: dict) -> bool:
    """
    Process a video message, same logic as receiver.py
    If metadata fails, falls back to unsorted collection.
    Returns True if file was added, False otherwise
    """
    try:
        file = message.video or message.document
        title = message.caption or file.file_name
        msg_id = message.id
        size = get_readable_file_size(file.file_size)
        channel = str(message.chat.id).replace("-100", "")
        
        metadata_info = await metadata(clean_filename(title), int(channel), msg_id)
        if metadata_info is None:
            LOGGER.warning(f"[ScanFiles] Metadata failed for video: {title} (ID: {msg_id})")
            stats["failed_metadata"] += 1
            # Fall back to unsorted collection
            result = await process_unsorted_message(bot, message, stats, error="Metadata lookup failed")
            return result
        
        # Store additional telegram metadata
        metadata_info['file_size_bytes'] = file.file_size
        metadata_info['telegram_date'] = to_utc_isoformat(message.date)
        
        title = remove_urls(title)
        if not title.endswith(('.mkv', '.mp4')):
            title += '.mkv'
        
        # Insert into database
        updated_id = await db.insert_media(metadata_info, channel=int(channel), msg_id=msg_id, size=size, name=title)
        
        if updated_id:
            if metadata_info.get('media_type') == 'movie':
                stats["movies_added"] += 1
            else:
                stats["tv_added"] += 1
            LOGGER.info(f"[ScanFiles] Added: {metadata_info.get('title')} ({metadata_info.get('media_type')})")
            return True
        else:
            stats["failed_insert"] += 1
            return False
            
    except Exception as e:
        LOGGER.error(f"[ScanFiles] Error processing video message {message.id}: {e}")
        stats["errors"] += 1
        return False


async def process_unsorted_message(bot: Client, message: Message, stats: dict, error: str = None) -> bool:
    """
    Process a non-video message and add to unsorted collection.
    Returns True if file was added, False otherwise.
    """
    try:
        from Backend.pyrofork.plugins.unsorted_receiver import handle_unsorted_file
        
        result = await handle_unsorted_file(bot, message, error=error)
        
        if result:
            stats["unsorted_added"] += 1
            LOGGER.info(f"[ScanFiles] Added to unsorted: message {message.id}")
            return True
        else:
            stats["skipped_unsorted"] += 1
            return False
            
    except Exception as e:
        LOGGER.error(f"[ScanFiles] Error processing unsorted message {message.id}: {e}")
        stats["errors"] += 1
        return False


# -------------------------------
# Callback Handlers
# -------------------------------
@Client.on_callback_query(filters.regex(r"^scan_cancel$"))
async def cancel_scan(_, query: CallbackQuery):
    global SCAN_CANCEL_REQUESTED
    SCAN_CANCEL_REQUESTED = True
    await query.answer("Cancelling...")


@Client.on_callback_query(filters.regex(r"^scan_channel_(.+)$"))
async def select_channel(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    channel_selection = query.data.split("_", 2)[2]  # "all" or channel_id
    
    SCAN_STATE[user_id] = {"channels": channel_selection}
    await query.answer()
    await show_mode_selection(query.message)


@Client.on_callback_query(filters.regex(r"^scan_mode_(date|count)$"))
async def select_mode(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    mode = query.data.split("_")[2]
    
    if user_id not in SCAN_STATE:
        await query.answer("Session expired. Please run /scan_files again.")
        return
    
    SCAN_STATE[user_id]["mode"] = mode
    await query.answer()
    
    if mode == "date":
        await show_date_options(query.message)
    else:
        await show_count_options(query.message)


@Client.on_callback_query(filters.regex(r"^scan_date_(.+)$"))
async def select_date_range(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    date_option = query.data.split("_", 2)[2]
    
    if user_id not in SCAN_STATE:
        await query.answer("Session expired. Please run /scan_files again.")
        return
    
    await query.answer()
    
    if date_option == "custom":
        SCAN_STATE[user_id]["awaiting_start_date"] = True
        await query.message.edit_text(
            "📅 **Custom Date Range**\n\n"
            "Enter the **start date** (YYYY-MM-DD):\n"
            "Example: `2025-12-01`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")]
            ])
        )
        return
    
    # Calculate date range (use local time to match Pyrogram's message.date)
    now = datetime.now()
    if date_option == "24h":
        start_date = now - timedelta(hours=24)
    elif date_option == "7d":
        start_date = now - timedelta(days=7)
    elif date_option == "30d":
        start_date = now - timedelta(days=30)
    elif date_option == "90d":
        start_date = now - timedelta(days=90)
    else:  # all
        start_date = None
    
    SCAN_STATE[user_id]["start_date"] = start_date
    SCAN_STATE[user_id]["end_date"] = now
    SCAN_STATE[user_id]["date_label"] = {
        "24h": "Last 24 hours",
        "7d": "Last 7 days",
        "30d": "Last 30 days",
        "90d": "Last 90 days",
        "all": "All Time"
    }.get(date_option, date_option)
    
    await start_scan(client, query.message, user_id)


@Client.on_callback_query(filters.regex(r"^scan_count_(.+)$"))
async def select_count(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    count_option = query.data.split("_", 2)[2]
    
    if user_id not in SCAN_STATE:
        await query.answer("Session expired. Please run /scan_files again.")
        return
    
    await query.answer()
    
    if count_option == "custom":
        SCAN_STATE[user_id]["awaiting_count"] = True
        await query.message.edit_text(
            "🔢 **Custom Message Count**\n\n"
            "Enter the number of messages to scan:\n"
            "Example: `250`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")]
            ])
        )
        return
    
    # Set count
    if count_option == "all":
        SCAN_STATE[user_id]["limit"] = None
        SCAN_STATE[user_id]["count_label"] = "All messages"
    else:
        SCAN_STATE[user_id]["limit"] = int(count_option)
        SCAN_STATE[user_id]["count_label"] = f"Last {count_option} messages"
    
    await start_scan(client, query.message, user_id)


@Client.on_callback_query(filters.regex(r"^scan_back_(.+)$"))
async def go_back(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    back_to = query.data.split("_", 2)[2]
    
    await query.answer()
    
    if back_to == "mode":
        await show_mode_selection(query.message)
    elif back_to == "channel":
        await show_channel_selection(query.message)


# -------------------------------
# UI Display Functions
# -------------------------------
async def show_channel_selection(message: Message):
    """Show channel selection if multiple AUTH_CHANNELs"""
    buttons = []
    
    for channel_id in Telegram.AUTH_CHANNEL:
        title = await get_channel_title(message._client, channel_id)
        buttons.append([InlineKeyboardButton(title, callback_data=f"scan_channel_{channel_id}")])
    
    buttons.append([InlineKeyboardButton("🌐 All Channels", callback_data="scan_channel_all")])
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")])
    
    await message.edit_text(
        "📂 **Select Channel to Scan**\n\n"
        "Choose which channel to scan for missed files:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def show_mode_selection(message: Message):
    """Show scan mode selection"""
    buttons = [
        [InlineKeyboardButton("📅 By Date Range", callback_data="scan_mode_date")],
        [InlineKeyboardButton("🔢 By Message Count", callback_data="scan_mode_count")],
    ]
    
    # Add back button only if multiple channels
    last_row = []
    if len(Telegram.AUTH_CHANNEL) > 1:
        last_row.append(InlineKeyboardButton("⬅️ Back", callback_data="scan_back_channel"))
    last_row.append(InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel"))
    buttons.append(last_row)
    
    await message.edit_text(
        "🔍 **How do you want to scan?**\n\n"
        "Choose your preferred scanning method:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def show_date_options(message: Message):
    """Show date range options"""
    await message.edit_text(
        "📅 **Select Time Period**\n\n"
        "Scan messages from:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Last 24 hours", callback_data="scan_date_24h"),
                InlineKeyboardButton("Last 7 days", callback_data="scan_date_7d")
            ],
            [
                InlineKeyboardButton("Last 30 days", callback_data="scan_date_30d"),
                InlineKeyboardButton("Last 90 days", callback_data="scan_date_90d")
            ],
            [
                InlineKeyboardButton("📅 Custom Range", callback_data="scan_date_custom"),
                InlineKeyboardButton("📜 All Time", callback_data="scan_date_all")
            ],
            [
                InlineKeyboardButton("⬅️ Back", callback_data="scan_back_mode"),
                InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")
            ]
        ])
    )


async def show_count_options(message: Message):
    """Show message count options"""
    await message.edit_text(
        "🔢 **How many messages to scan?**\n\n"
        "Select the number of recent messages to check:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("100", callback_data="scan_count_100"),
                InlineKeyboardButton("500", callback_data="scan_count_500"),
                InlineKeyboardButton("1000", callback_data="scan_count_1000")
            ],
            [
                InlineKeyboardButton("✏️ Custom", callback_data="scan_count_custom"),
                InlineKeyboardButton("📜 All", callback_data="scan_count_all")
            ],
            [
                InlineKeyboardButton("⬅️ Back", callback_data="scan_back_mode"),
                InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")
            ]
        ])
    )


# -------------------------------
# Main Scan Function
# -------------------------------
async def start_scan(client: Client, status_message: Message, user_id: int):
    """Execute the scan operation"""
    global SCAN_CANCEL_REQUESTED
    SCAN_CANCEL_REQUESTED = False
    
    state = SCAN_STATE.get(user_id, {})
    if not state:
        await status_message.edit_text("❌ Session expired. Please run /scan_files again.")
        return
    
    # Determine channels to scan
    channel_selection = state.get("channels", "all")
    if channel_selection == "all":
        channels_to_scan = Telegram.AUTH_CHANNEL
    else:
        channels_to_scan = [channel_selection]
    
    # Get scan parameters
    mode = state.get("mode")
    start_date = state.get("start_date")
    end_date = state.get("end_date")
    limit = state.get("limit")
    
    # Build mode label
    if mode == "date":
        mode_label = state.get("date_label", "Date range")
    else:
        mode_label = state.get("count_label", f"Last {limit} messages")
    
    # Initialize stats
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
    
    start_time = time.time()
    last_progress_update = start_time
    PROGRESS_INTERVAL = 2.0
    RATE_LIMIT_DELAY = 0.3
    
    # Build existing message IDs set
    await status_message.edit_text(
        "🔄 **Preparing scan...**\n\n"
        "Building index of existing files in database...",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")]
        ])
    )
    
    existing_msg_ids, video_msg_ids, unsorted_msg_ids = await build_existing_msg_ids()
    LOGGER.info(f"[ScanFiles] Found {len(existing_msg_ids)} existing entries (Videos: {len(video_msg_ids)}, Unsorted: {len(unsorted_msg_ids)})")
    
    if SCAN_CANCEL_REQUESTED:
        await status_message.edit_text("❌ Scan cancelled.")
        if user_id in SCAN_STATE:
            del SCAN_STATE[user_id]
        return
    
    # Get channel titles for display
    channel_titles = []
    for ch in channels_to_scan:
        title = await get_channel_title(client, ch)
        channel_titles.append(title)
    
    channels_display = "\n   • ".join(channel_titles) if len(channel_titles) > 1 else channel_titles[0]
    
    # Scan channels
    for channel_id in channels_to_scan:
        if SCAN_CANCEL_REQUESTED:
            break
        
        chat_id = normalize_channel_id(channel_id)
        raw_channel_id = get_raw_channel_id(channel_id)
        channel_title = await get_channel_title(client, channel_id)
        
        try:
            # Find the highest message ID we know about in this channel from DB
            max_known_msg_id = 0
            for (ch, msg_id) in existing_msg_ids:
                if ch == raw_channel_id and msg_id > max_known_msg_id:
                    max_known_msg_id = msg_id
            
            LOGGER.info(f"[ScanFiles] Channel {raw_channel_id}: max known msg_id from DB = {max_known_msg_id}")
            
            # Calculate scan range using SCAN_BUFFER to handle gaps from deletions
            # upper_bound: max_known + buffer (catches new messages beyond what we know)
            # lower_bound: always 1, we rely on limit/date checks to stop early
            upper_bound = max_known_msg_id + SCAN_BUFFER
            lower_bound = 1
            
            LOGGER.info(f"[ScanFiles] Scan range: {lower_bound} to {upper_bound}")
            
            # Scan messages in batches (Telegram allows up to 200 per get_messages call)
            BATCH_SIZE = 200
            current_id = upper_bound
            messages_scanned = 0
            scan_complete = False  # Flag for early exit in date mode
            
            while current_id >= lower_bound and not scan_complete:
                if SCAN_CANCEL_REQUESTED:
                    break
                
                # For count mode, stop if we've checked enough messages
                if mode == "count" and limit and stats["messages_checked"] >= limit:
                    break
                
                # Calculate batch range
                batch_end = current_id
                batch_start = max(lower_bound, current_id - BATCH_SIZE + 1)
                msg_ids = list(range(batch_start, batch_end + 1))
                
                if not msg_ids:
                    break
                
                # Fetch batch of messages
                try:
                    messages = await client.get_messages(chat_id, msg_ids)
                    if not isinstance(messages, list):
                        messages = [messages] if messages else []
                except Exception as e:
                    LOGGER.debug(f"[ScanFiles] Error fetching batch {batch_start}-{batch_end}: {e}")
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
                    if SCAN_CANCEL_REQUESTED:
                        break
                    
                    messages_scanned += 1
                    
                    # Date-based filtering with early exit optimization
                    # Since we're iterating newest-to-oldest, once we hit an old message,
                    # all remaining messages in this batch are also old - so we can break.
                    if mode == "date":
                        msg_date = message.date.replace(tzinfo=None) if message.date else None
                        # If message is older than start_date, we're done (scanning newest to oldest)
                        if start_date and msg_date and msg_date < start_date:
                            scan_complete = True
                            break
                        # Skip messages newer than end_date
                        if end_date and msg_date and msg_date > end_date:
                            continue
                    
                    stats["messages_checked"] += 1
                    
                    # Classify message FIRST (before checking limit)
                    msg_key = (raw_channel_id, message.id)
                    if msg_key in existing_msg_ids:
                        stats["already_processed"] += 1
                        # Track breakdown: was it a video or unsorted?
                        if msg_key in video_msg_ids:
                            stats["already_processed_videos"] += 1
                        elif msg_key in unsorted_msg_ids:
                            stats["already_processed_unsorted"] += 1
                    elif message.video or (message.document and message.document.mime_type and message.document.mime_type.startswith("video/")):
                        # Video file -> try movie/TV, fallback to unsorted
                        await process_video_message(client, message, stats)
                        await asyncio.sleep(RATE_LIMIT_DELAY)
                    elif message.document or message.audio or message.voice or message.video_note or message.animation:
                        # Non-video file -> unsorted collection
                        await process_unsorted_message(client, message, stats)
                        await asyncio.sleep(RATE_LIMIT_DELAY)
                    else:
                        # No file attachment
                        stats["skipped_no_file"] += 1
                    
                    # Count mode: stop AFTER classifying the message
                    if mode == "count" and limit and stats["messages_checked"] >= limit:
                        scan_complete = True
                        break
                    
                    # Update progress
                    now = time.time()
                    if now - last_progress_update > PROGRESS_INTERVAL:
                        last_progress_update = now
                        await update_progress(status_message, channel_title, mode_label, stats, start_time, limit)
                
                current_id = batch_start - 1
                await asyncio.sleep(0.1)  # Small delay between batches
        
        except FloodWait as e:
            LOGGER.warning(f"[ScanFiles] FloodWait: sleeping {e.value}s")
            await asyncio.sleep(e.value)
        except Exception as e:
            LOGGER.error(f"[ScanFiles] Error scanning channel {channel_id}: {e}")
            stats["errors"] += 1
    
    # Final report
    elapsed = time.time() - start_time
    total_videos = stats["movies_added"] + stats["tv_added"]
    total_added = total_videos + stats["unsorted_added"]
    
    if SCAN_CANCEL_REQUESTED:
        report = (
            f"⚠️ **Scan Cancelled**\n\n"
            f"📊 **Channel{'s' if len(channels_to_scan) > 1 else ''}:** {channels_display}\n"
            f"📅 **Mode:** {mode_label}\n"
            f"🎯 **Target:** Videos + Unsorted Files\n\n"
            f"**Processed before cancel:**\n"
            f"📁 Messages checked: {format_number(stats['messages_checked'])}\n"
            f"📁 New files added: {total_added}\n"
        )
        if total_added > 0:
            report += (
                f"   ├─ 🎬 Movies: {stats['movies_added']}\n"
                f"   ├─ 📺 TV Episodes: {stats['tv_added']}\n"
                f"   └─ 📁 Unsorted: {stats['unsorted_added']}\n"
            )
        report += f"\n⏭️ Already processed: {format_number(stats['already_processed'])}\n"
        if stats['already_processed'] > 0:
            report += (
                f"   ├─ 🎬 Videos: {format_number(stats['already_processed_videos'])}\n"
                f"   └─ 📁 Unsorted: {format_number(stats['already_processed_unsorted'])}\n"
            )
        report += (
            f"🚫 Skipped (no media or file): {format_number(stats['skipped_no_file'])}\n"
            f"⚠️ Failed (metadata), moved to unsorted: {format_number(stats['failed_metadata'])}"
        )
    else:
        if stats["messages_checked"] == 0:
            report = (
                f"✅ **Scan Complete!**\n\n"
                f"📊 **Channel{'s' if len(channels_to_scan) > 1 else ''}:** {channels_display}\n"
                f"📅 **Mode:** {mode_label}\n"
                f"🎯 **Target:** Videos + Unsorted Files\n\n"
                f"⚠️ No messages found in this range."
            )
        else:
            report = (
                f"✅ **Scan Complete!**\n\n"
                f"📊 **Channel{'s' if len(channels_to_scan) > 1 else ''}:**\n   • {channels_display}\n"
                f"📅 **Mode:** {mode_label} ({format_number(stats['messages_checked'])} messages found)\n"
                f"🎯 **Target:** Videos + Unsorted Files\n\n"
                f"📁 **New files added:** {total_added}\n"
            )
            if total_added > 0:
                report += (
                    f"   ├─ 🎬 Movies: {stats['movies_added']}\n"
                    f"   ├─ 📺 TV Episodes: {stats['tv_added']}\n"
                    f"   └─ 📁 Unsorted: {stats['unsorted_added']}\n"
                )
            report += f"\n⏭️ Already processed: {format_number(stats['already_processed'])}\n"
            if stats['already_processed'] > 0:
                report += (
                    f"   ├─ 🎬 Videos: {format_number(stats['already_processed_videos'])}\n"
                    f"   └─ 📁 Unsorted: {format_number(stats['already_processed_unsorted'])}\n"
                )
            report += (
                f"🚫 Skipped (no media or file): {format_number(stats['skipped_no_file'])}\n"
                f"⚠️ Failed (metadata), moved to unsorted: {format_number(stats['failed_metadata'])}\n"
                f"⏱️ Time taken: {format_eta(elapsed)}"
            )
            
            if total_added == 0 and stats["already_processed"] > 0:
                report += "\n\nℹ️ All files in this range are already in the database."
    
    await status_message.edit_text(report)
    
    # Cleanup state
    if user_id in SCAN_STATE:
        del SCAN_STATE[user_id]
    
    LOGGER.info(f"[ScanFiles] Completed - Movies: {stats['movies_added']}, TV: {stats['tv_added']}, Already: {stats['already_processed']}")


async def update_progress(message: Message, channel: str, mode_label: str, stats: dict, start_time: float, limit: int = None):
    """Update progress message with visual progress bar"""
    total_added = stats["movies_added"] + stats["tv_added"]
    elapsed = format_eta(time.time() - start_time)
    checked = stats["messages_checked"]
    
    # Build progress bar
    if limit and limit > 0:
        # Count mode: show actual progress bar
        bar = progress_bar(checked, limit)
    else:
        # Date mode: show indeterminate animated bar
        # Cycle through different fill levels based on checked count
        fill_level = (checked // 10) % 20
        bar = f"[{'█' * fill_level}{'░' * (20 - fill_level)}] {format_number(checked)} checked"
    
    try:
        await message.edit_text(
            f"🔍 **Scanning {channel}...**\n\n"
            f"📅 Mode: {mode_label}\n"
            f"📊 {bar}\n"
            f"📁 Found: {total_added} new files\n"
            f"⏭️ Skipped: {format_number(stats['already_processed'])}\n"
            f"⏱️ Elapsed: {elapsed}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")]
            ])
        )
    except Exception:
        pass  # Ignore edit errors (e.g., message not modified)


# -------------------------------
# Text Input Handler (for custom values)
# -------------------------------
@Client.on_message(filters.private & filters.text & CustomFilters.owner, group=15)
async def handle_text_input(client: Client, message: Message):
    """Handle text input for custom date/count"""
    user_id = message.from_user.id
    
    if user_id not in SCAN_STATE:
        return
    
    state = SCAN_STATE[user_id]
    
    # Handle custom start date input
    if state.get("awaiting_start_date"):
        try:
            start_date = datetime.strptime(message.text.strip(), "%Y-%m-%d")
            state["start_date"] = start_date
            state["awaiting_start_date"] = False
            state["awaiting_end_date"] = True
            
            await message.reply_text(
                f"✅ Start date: **{start_date.strftime('%b %d, %Y')}**\n\n"
                "Enter the **end date** (YYYY-MM-DD) or type `now` for today:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")]
                ])
            )
        except ValueError:
            await message.reply_text(
                "❌ Invalid date format. Please use YYYY-MM-DD\n"
                "Example: `2025-12-01`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")]
                ])
            )
        return
    
    # Handle custom end date input
    if state.get("awaiting_end_date"):
        try:
            text = message.text.strip().lower()
            if text == "now":
                end_date = datetime.now()
            else:
                end_date = datetime.strptime(text, "%Y-%m-%d")
                # Set to end of day
                end_date = end_date.replace(hour=23, minute=59, second=59)
            
            state["end_date"] = end_date
            state["awaiting_end_date"] = False
            state["date_label"] = f"{state['start_date'].strftime('%b %d, %Y')} → {end_date.strftime('%b %d, %Y')}"
            
            status_msg = await message.reply_text("✅ Date range set. Starting scan...")
            await start_scan(client, status_msg, user_id)
        except ValueError:
            await message.reply_text(
                "❌ Invalid date format. Please use YYYY-MM-DD or type `now`\n"
                "Example: `2025-12-15`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")]
                ])
            )
        return
    
    # Handle custom count input
    if state.get("awaiting_count"):
        try:
            count = int(message.text.strip())
            if count <= 0:
                raise ValueError("Count must be positive")
            
            state["limit"] = count
            state["count_label"] = f"Last {format_number(count)} messages"
            state["awaiting_count"] = False
            
            status_msg = await message.reply_text(f"✅ Scanning last {format_number(count)} messages...")
            await start_scan(client, status_msg, user_id)
        except ValueError:
            await message.reply_text(
                "❌ Please enter a valid positive number.\n"
                "Example: `250`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")]
                ])
            )
        return


# -------------------------------
# Main Command Handler
# -------------------------------
@Client.on_message(filters.command("scan_files") & filters.private & CustomFilters.owner, group=12)
async def scan_files_handler(client: Client, message: Message):
    """Entry point for /scan_files command"""
    global SCAN_CANCEL_REQUESTED
    SCAN_CANCEL_REQUESTED = False
    
    user_id = message.from_user.id
    
    # Clear any previous state
    if user_id in SCAN_STATE:
        del SCAN_STATE[user_id]
    
    # Check if AUTH_CHANNEL is configured
    if not Telegram.AUTH_CHANNEL:
        await message.reply_text(
            "❌ **No channels configured**\n\n"
            "Please set AUTH_CHANNEL in your environment variables."
        )
        return
    
    # Initialize state
    SCAN_STATE[user_id] = {}
    
    # If single channel, skip channel selection
    if len(Telegram.AUTH_CHANNEL) == 1:
        SCAN_STATE[user_id]["channels"] = Telegram.AUTH_CHANNEL[0]
        status_msg = await message.reply_text(
            "📂 **Scan Files**\n\n"
            "Preparing...",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")]
            ])
        )
        await show_mode_selection(status_msg)
    else:
        # Multiple channels - show selection
        status_msg = await message.reply_text(
            "📂 **Scan Files**\n\n"
            "Preparing...",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="scan_cancel")]
            ])
        )
        await show_channel_selection(status_msg)

