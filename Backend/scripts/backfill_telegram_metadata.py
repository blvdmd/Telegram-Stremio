#!/usr/bin/env python3
"""
Backfill script to populate existing telegram objects with sizeInBytes, updated_on, and created_on.

This script:
1. Connects to the database
2. Iterates through all movies and TV shows
3. For each telegram object, decodes the ID to get chat_id and msg_id
4. Fetches the message from Telegram to get file_size and message.date
5. Updates the database with the new fields

Usage:
    python -m Backend.scripts.backfill_telegram_metadata

Note: Run this from the project root directory after setting up environment variables.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyrogram import Client
from Backend.config import Telegram
from Backend.helper.database import Database
from Backend.helper.encrypt import decode_string
from Backend.logger import LOGGER


async def backfill_movie_telegram_metadata(db: Database, bot: Client, movie: dict, db_key: str):
    """Backfill telegram metadata for a single movie"""
    tmdb_id = movie.get('tmdb_id')
    telegram_list = movie.get('telegram', [])
    updated = False
    
    for telegram_item in telegram_list:
        # Skip if already has the metadata
        if telegram_item.get('sizeInBytes') is not None:
            continue
            
        telegram_id = telegram_item.get('id')
        if not telegram_id:
            continue
            
        try:
            decoded = await decode_string(telegram_id)
            chat_id = int(f"-100{decoded['chat_id']}")
            msg_id = int(decoded['msg_id'])
            
            # Fetch message from Telegram
            message = await bot.get_messages(chat_id, msg_id)
            if message and (message.video or message.document):
                file = message.video or message.document
                
                # Update telegram item with metadata
                telegram_item['sizeInBytes'] = file.file_size
                telegram_item['created_on'] = message.date.isoformat() if message.date else None
                telegram_item['updated_on'] = message.date.isoformat() if message.date else None
                updated = True
                
                LOGGER.info(f"[Movie] Updated metadata for {movie.get('title')} - {telegram_item.get('quality')}")
            else:
                LOGGER.warning(f"[Movie] Message not found for {movie.get('title')} - msg_id: {msg_id}")
                
        except Exception as e:
            LOGGER.error(f"[Movie] Error processing {movie.get('title')}: {e}")
            continue
    
    if updated:
        # Save the updated movie back to database
        try:
            collection = db.dbs[db_key]["movie"]
            await collection.update_one(
                {"tmdb_id": tmdb_id},
                {"$set": {"telegram": telegram_list}}
            )
            LOGGER.info(f"[Movie] Saved updates for: {movie.get('title')}")
        except Exception as e:
            LOGGER.error(f"[Movie] Failed to save updates for {movie.get('title')}: {e}")


async def backfill_tv_telegram_metadata(db: Database, bot: Client, tv_show: dict, db_key: str):
    """Backfill telegram metadata for a single TV show"""
    tmdb_id = tv_show.get('tmdb_id')
    seasons = tv_show.get('seasons', [])
    updated = False
    
    for season in seasons:
        for episode in season.get('episodes', []):
            telegram_list = episode.get('telegram', [])
            
            for telegram_item in telegram_list:
                # Skip if already has the metadata
                if telegram_item.get('sizeInBytes') is not None:
                    continue
                    
                telegram_id = telegram_item.get('id')
                if not telegram_id:
                    continue
                    
                try:
                    decoded = await decode_string(telegram_id)
                    chat_id = int(f"-100{decoded['chat_id']}")
                    msg_id = int(decoded['msg_id'])
                    
                    # Fetch message from Telegram
                    message = await bot.get_messages(chat_id, msg_id)
                    if message and (message.video or message.document):
                        file = message.video or message.document
                        
                        # Update telegram item with metadata
                        telegram_item['sizeInBytes'] = file.file_size
                        telegram_item['created_on'] = message.date.isoformat() if message.date else None
                        telegram_item['updated_on'] = message.date.isoformat() if message.date else None
                        updated = True
                        
                        LOGGER.info(
                            f"[TV] Updated metadata for {tv_show.get('title')} "
                            f"S{season.get('season_number')}E{episode.get('episode_number')} - {telegram_item.get('quality')}"
                        )
                    else:
                        LOGGER.warning(
                            f"[TV] Message not found for {tv_show.get('title')} "
                            f"S{season.get('season_number')}E{episode.get('episode_number')} - msg_id: {msg_id}"
                        )
                        
                except Exception as e:
                    LOGGER.error(
                        f"[TV] Error processing {tv_show.get('title')} "
                        f"S{season.get('season_number')}E{episode.get('episode_number')}: {e}"
                    )
                    continue
    
    if updated:
        # Save the updated TV show back to database
        try:
            collection = db.dbs[db_key]["tv"]
            await collection.update_one(
                {"tmdb_id": tmdb_id},
                {"$set": {"seasons": seasons}}
            )
            LOGGER.info(f"[TV] Saved updates for: {tv_show.get('title')}")
        except Exception as e:
            LOGGER.error(f"[TV] Failed to save updates for {tv_show.get('title')}: {e}")


async def main():
    """Main backfill function"""
    LOGGER.info("=" * 60)
    LOGGER.info("Starting Telegram Metadata Backfill Script")
    LOGGER.info("=" * 60)
    
    # Initialize database
    db = Database()
    await db.connect()
    
    # Initialize Telegram bot client
    bot = Client(
        name='backfill_bot',
        api_id=Telegram.API_ID,
        api_hash=Telegram.API_HASH,
        bot_token=Telegram.BOT_TOKEN,
    )
    
    async with bot:
        LOGGER.info("Connected to Telegram")
        
        # Process all storage databases
        total_storage_dbs = len(db.dbs) - 1  # Exclude tracking db
        
        for db_index in range(1, total_storage_dbs + 1):
            db_key = f"storage_{db_index}"
            LOGGER.info(f"\n--- Processing {db_key} ---")
            
            # Process movies
            movie_collection = db.dbs[db_key]["movie"]
            movies_cursor = movie_collection.find({})
            movies = await movies_cursor.to_list(None)
            
            LOGGER.info(f"Found {len(movies)} movies in {db_key}")
            for movie in movies:
                await backfill_movie_telegram_metadata(db, bot, movie, db_key)
                await asyncio.sleep(0.5)  # Rate limiting
            
            # Process TV shows
            tv_collection = db.dbs[db_key]["tv"]
            tv_cursor = tv_collection.find({})
            tv_shows = await tv_cursor.to_list(None)
            
            LOGGER.info(f"Found {len(tv_shows)} TV shows in {db_key}")
            for tv_show in tv_shows:
                await backfill_tv_telegram_metadata(db, bot, tv_show, db_key)
                await asyncio.sleep(0.5)  # Rate limiting
    
    await db.disconnect()
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Backfill Script Completed")
    LOGGER.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

