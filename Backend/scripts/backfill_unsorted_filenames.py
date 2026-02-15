#!/usr/bin/env python3
"""
Backfill script to re-extract filenames for existing unsorted files.

This script:
1. Connects to the database
2. Iterates all documents in the unsorted collection(s)
3. For each doc with a caption and file_extension, runs extract_best_filename()
4. If the result differs from the current file_name, updates the document in-place

Usage:
    python -m Backend.scripts.backfill_unsorted_filenames

Note: Run this from the project root directory after setting up environment variables.
"""

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from datetime import datetime
from Backend.helper.database import Database
from Backend.helper.unsorted_collection import UnsortedCollection
from Backend.helper.unsorted_modal import extract_best_filename
from Backend.logger import LOGGER


async def backfill_filenames(unsorted: UnsortedCollection):
    """
    Re-extract filenames for all existing unsorted files.
    
    For each document, runs extract_best_filename(file_name, caption, file_extension).
    If the result differs from the current file_name, updates the document.
    """
    total_checked = 0
    updated = 0
    unchanged = 0
    errors = 0

    for db_key in unsorted.dbs:
        collection = unsorted.dbs[db_key][unsorted.COLLECTION_NAME]

        doc_count = await collection.count_documents({})
        LOGGER.info(f"[Backfill] {db_key}: {doc_count} documents to check")

        cursor = collection.find(
            {},
            {"_id": 1, "file_name": 1, "caption": 1, "file_extension": 1}
        )

        async for doc in cursor:
            total_checked += 1
            try:
                old_name = doc.get("file_name", "")
                caption = doc.get("caption")
                ext = doc.get("file_extension", "")

                new_name = extract_best_filename(old_name, caption, ext)

                if new_name != old_name:
                    await collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"file_name": new_name, "updated_on": datetime.utcnow()}}
                    )
                    updated += 1
                    LOGGER.info(f"[Backfill] Updated: {old_name!r} -> {new_name!r}")
                else:
                    unchanged += 1
            except Exception as e:
                LOGGER.error(f"[Backfill] Error on doc {doc.get('_id')}: {e}")
                errors += 1

        LOGGER.info(f"[Backfill] {db_key} done. Checked so far: {total_checked}")

    return total_checked, updated, unchanged, errors


async def main():
    """Main backfill entry point."""
    LOGGER.info("=" * 60)
    LOGGER.info("Starting Unsorted Filenames Backfill Script")
    LOGGER.info("=" * 60)

    # Initialize database
    db = Database()
    await db.connect()

    unsorted = UnsortedCollection(db)

    try:
        total_checked, updated, unchanged, errors = await backfill_filenames(unsorted)

        LOGGER.info("")
        LOGGER.info("=" * 60)
        LOGGER.info("Backfill Complete")
        LOGGER.info(f"  Total checked : {total_checked}")
        LOGGER.info(f"  Updated       : {updated}")
        LOGGER.info(f"  Unchanged     : {unchanged}")
        LOGGER.info(f"  Errors        : {errors}")
        LOGGER.info("=" * 60)
    finally:
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
