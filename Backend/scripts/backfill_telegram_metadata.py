#!/usr/bin/env python3
"""
Backfill script to populate existing telegram objects with size_bytes, updated_on, and created_on.

This script:
1. Migrates existing 'sizeInBytes' fields to 'size_bytes' (one-time migration)
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

from datetime import datetime
from pyrogram import Client
from Backend.config import Telegram
from Backend.helper.database import Database
from Backend.helper.encrypt import decode_string
from Backend.helper.pyro import to_utc_isoformat
from Backend.logger import LOGGER


async def migrate_size_field(db: Database):
    """
    One-time migration: Rename 'sizeInBytes' to 'size_bytes' in all telegram objects.
    This handles the field naming convention change.
    """
    LOGGER.info("=" * 60)
    LOGGER.info("Starting Field Migration: sizeInBytes -> size_bytes")
    LOGGER.info("=" * 60)
    
    total_storage_dbs = len(db.dbs) - 1  # Exclude tracking db
    total_movies_updated = 0
    total_tv_updated = 0
    
    for db_index in range(1, total_storage_dbs + 1):
        db_key = f"storage_{db_index}"
        LOGGER.info(f"\n--- Migrating {db_key} ---")
        
        movies_in_db = 0
        tv_in_db = 0
        
        # Migrate movies: rename telegram[].sizeInBytes to telegram[].size_bytes
        # Note: MongoDB $rename doesn't work with array positional operators, so we iterate manually
        movie_collection = db.dbs[db_key]["movie"]
        movies_cursor = movie_collection.find({"telegram.sizeInBytes": {"$exists": True}})
        async for movie in movies_cursor:
            telegram_list = movie.get('telegram', [])
            updated = False
            for item in telegram_list:
                if 'sizeInBytes' in item:
                    item['size_bytes'] = item.pop('sizeInBytes')
                    updated = True
            if updated:
                await movie_collection.update_one(
                    {"_id": movie["_id"]},
                    {"$set": {"telegram": telegram_list}}
                )
                movies_in_db += 1
                total_movies_updated += 1
        
        LOGGER.info(f"[{db_key}] Migrated {movies_in_db} movies")
        
        # Migrate TV shows: rename seasons[].episodes[].telegram[].sizeInBytes
        # Note: MongoDB $rename doesn't work with nested arrays, so we iterate manually
        tv_collection = db.dbs[db_key]["tv"]
        tv_cursor = tv_collection.find({"seasons.episodes.telegram.sizeInBytes": {"$exists": True}})
        async for tv_show in tv_cursor:
            seasons = tv_show.get('seasons', [])
            updated = False
            for season in seasons:
                for episode in season.get('episodes', []):
                    for item in episode.get('telegram', []):
                        if 'sizeInBytes' in item:
                            item['size_bytes'] = item.pop('sizeInBytes')
                            updated = True
            if updated:
                await tv_collection.update_one(
                    {"_id": tv_show["_id"]},
                    {"$set": {"seasons": seasons}}
                )
                tv_in_db += 1
                total_tv_updated += 1
        
        LOGGER.info(f"[{db_key}] Migrated {tv_in_db} TV shows")
    
    LOGGER.info(f"\nMigration complete: {total_movies_updated} movies, {total_tv_updated} TV shows updated")
    LOGGER.info("=" * 60)


async def backfill_movie_telegram_metadata(db: Database, bot: Client, movie: dict, db_key: str):
    """Backfill telegram metadata for a single movie"""
    tmdb_id = movie.get('tmdb_id')
    telegram_list = movie.get('telegram', [])
    updated = False
    
    for telegram_item in telegram_list:
        # Skip if already has the metadata
        if telegram_item.get('size_bytes') is not None:
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
            
            # Support all media types (video, document, audio, voice, video_note, animation)
            file = (
                message.document or
                message.video or
                message.audio or
                message.voice or
                message.video_note or
                message.animation
            ) if message else None
            
            if file:
                # Update telegram item with metadata (using consistent UTC format)
                telegram_item['size_bytes'] = file.file_size
                telegram_item['created_on'] = to_utc_isoformat(message.date)
                telegram_item['updated_on'] = to_utc_isoformat(message.date)
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
                if telegram_item.get('size_bytes') is not None:
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
                    
                    # Support all media types (video, document, audio, voice, video_note, animation)
                    file = (
                        message.document or
                        message.video or
                        message.audio or
                        message.voice or
                        message.video_note or
                        message.animation
                    ) if message else None
                    
                    if file:
                        # Update telegram item with metadata (using consistent UTC format)
                        telegram_item['size_bytes'] = file.file_size
                        telegram_item['created_on'] = to_utc_isoformat(message.date)
                        telegram_item['updated_on'] = to_utc_isoformat(message.date)
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
    
    # Step 1: Run one-time field migration (sizeInBytes -> size_bytes)
    await migrate_size_field(db)
    
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
            
            # Process movies - use batched iteration to avoid memory issues
            movie_collection = db.dbs[db_key]["movie"]
            movie_count = await movie_collection.count_documents({})
            LOGGER.info(f"Found {movie_count} movies in {db_key}")
            
            # Process in batches of 100 to limit memory usage
            batch_size = 100
            movies_cursor = movie_collection.find({}).batch_size(batch_size)
            async for movie in movies_cursor:
                await backfill_movie_telegram_metadata(db, bot, movie, db_key)
                await asyncio.sleep(0.5)  # Rate limiting
            
            # Process TV shows - use batched iteration to avoid memory issues
            tv_collection = db.dbs[db_key]["tv"]
            tv_count = await tv_collection.count_documents({})
            LOGGER.info(f"Found {tv_count} TV shows in {db_key}")
            
            tv_cursor = tv_collection.find({}).batch_size(batch_size)
            async for tv_show in tv_cursor:
                await backfill_tv_telegram_metadata(db, bot, tv_show, db_key)
                await asyncio.sleep(0.5)  # Rate limiting
    
    await db.disconnect()
    LOGGER.info("\n" + "=" * 60)
    LOGGER.info("Backfill Script Completed")
    LOGGER.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
