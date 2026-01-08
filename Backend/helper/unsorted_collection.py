"""
Operations for the 'unsorted' collection within the existing database.
Isolated from main database.py to avoid merge conflicts with upstream.

This module manages a separate COLLECTION (not database) called 'unsorted'
within the same MongoDB databases used by movies and TV shows.
"""

from asyncio import Lock
from bson import ObjectId
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pymongo import ASCENDING, DESCENDING

from Backend.logger import LOGGER
from Backend.helper.encrypt import decode_string
from Backend.helper.task_manager import delete_message


# Lock for thread-safe database writes
_db_write_lock = Lock()


def convert_objectid_to_str(document: Dict[str, Any]) -> Dict[str, Any]:
    """Convert ObjectId and datetime fields to strings for JSON serialization."""
    if document is None:
        return None
    # Create a copy to avoid mutating the original
    result = {}
    for key, value in document.items():
        if isinstance(value, ObjectId):
            result[key] = str(value)
        elif isinstance(value, datetime):
            # Use strftime for consistent format with microseconds
            result[key] = value.strftime('%Y-%m-%dT%H:%M:%S.%f')
        elif isinstance(value, list):
            result[key] = [
                convert_objectid_to_str(item) if isinstance(item, dict) else item 
                for item in value
            ]
        elif isinstance(value, dict):
            result[key] = convert_objectid_to_str(value)
        else:
            result[key] = value
    return result


class UnsortedCollection:
    """
    Manager for the 'unsorted' collection within the existing database.
    
    This class uses the SAME database instances from the main Database class
    but operates on a separate COLLECTION to keep data isolated.
    The unsorted files are stored alongside movies and TV shows in the same DB.
    """
    
    COLLECTION_NAME = "unsorted"
    
    def __init__(self, main_db):
        """
        Initialize with reference to main database instance.
        
        Args:
            main_db: The main Database instance from Backend.helper.database
        """
        self.main_db = main_db
    
    @property
    def dbs(self):
        """Access to storage databases."""
        return self.main_db.dbs
    
    @property
    def current_db_index(self):
        """Current active storage database index."""
        return self.main_db.current_db_index
    
    async def ensure_indexes(self):
        """Create indexes for efficient queries."""
        try:
            for i in range(1, self.current_db_index + 1):
                db_key = f"storage_{i}"
                collection = self.dbs[db_key][self.COLLECTION_NAME]
                
                # Index for sorting by date
                await collection.create_index([("created_on", DESCENDING)])
                
                # Index for filtering by media type
                await collection.create_index([("media_type", ASCENDING)])
                
                # Index for filename search
                await collection.create_index([("file_name", ASCENDING)])
                
                # Compound index for common query patterns
                await collection.create_index([
                    ("media_type", ASCENDING),
                    ("created_on", DESCENDING)
                ])
                
            LOGGER.info("[UnsortedDB] Indexes created successfully")
        except Exception as e:
            LOGGER.error(f"[UnsortedDB] Failed to create indexes: {e}")
    
    async def insert_file(self, file_data: dict) -> Optional[ObjectId]:
        """
        Insert a new unsorted file into the database.
        
        Args:
            file_data: Dictionary with file information
            
        Returns:
            ObjectId of inserted document, or None on failure
        """
        async with _db_write_lock:
            try:
                db_key = f"storage_{self.current_db_index}"
                collection = self.dbs[db_key][self.COLLECTION_NAME]
                
                # Ensure timestamps
                now = datetime.utcnow()
                file_data.setdefault("created_on", now)
                file_data.setdefault("updated_on", now)
                file_data["db_index"] = self.current_db_index
                
                result = await collection.insert_one(file_data)
                LOGGER.info(f"[UnsortedDB] Inserted file: {file_data.get('file_name')}")
                return result.inserted_id
                
            except Exception as e:
                LOGGER.error(f"[UnsortedDB] Insert failed: {e}")
                # Handle storage quota errors
                if any(keyword in str(e).lower() for keyword in ["storage", "quota"]):
                    LOGGER.warning("[UnsortedDB] Storage quota exceeded")
                return None
    
    async def get_file(self, file_id: str, db_index: int) -> Optional[dict]:
        """
        Get a single file by ID.
        
        Args:
            file_id: MongoDB ObjectId as string
            db_index: Database index where file is stored
            
        Returns:
            File document or None
        """
        try:
            db_key = f"storage_{db_index}"
            collection = self.dbs[db_key][self.COLLECTION_NAME]
            
            doc = await collection.find_one({"_id": ObjectId(file_id)})
            return convert_objectid_to_str(doc) if doc else None
            
        except Exception as e:
            LOGGER.error(f"[UnsortedDB] Get file failed: {e}")
            return None
    
    async def delete_file(self, file_id: str, db_index: int) -> bool:
        """
        Delete a file from database AND from Telegram channel.
        
        Args:
            file_id: MongoDB ObjectId as string
            db_index: Database index where file is stored
            
        Returns:
            True if deleted successfully
        """
        try:
            db_key = f"storage_{db_index}"
            collection = self.dbs[db_key][self.COLLECTION_NAME]
            
            # Get file first to extract telegram info
            doc = await collection.find_one({"_id": ObjectId(file_id)})
            if not doc:
                LOGGER.warning(f"[UnsortedDB] File not found for deletion: {file_id}")
                return False
            
            # Delete from Telegram channel
            telegram_id = doc.get("telegram_id")
            if telegram_id:
                try:
                    decoded = await decode_string(telegram_id)
                    chat_id = int(f"-100{decoded['chat_id']}")
                    msg_id = int(decoded['msg_id'])
                    # Await deletion directly (safer than fire-and-forget task)
                    await delete_message(chat_id, msg_id)
                    LOGGER.info(f"[UnsortedDB] Deleted Telegram message: {msg_id}")
                except Exception as e:
                    LOGGER.error(f"[UnsortedDB] Failed to delete Telegram message: {e}")
            
            # Delete from database
            result = await collection.delete_one({"_id": ObjectId(file_id)})
            
            if result.deleted_count > 0:
                LOGGER.info(f"[UnsortedDB] Deleted file: {doc.get('file_name')}")
                return True
            
            return False
            
        except Exception as e:
            LOGGER.error(f"[UnsortedDB] Delete failed: {e}")
            return False
    
    async def delete_files_bulk(self, file_ids: List[Tuple[str, int]]) -> dict:
        """
        Delete multiple files from database AND Telegram.
        
        Args:
            file_ids: List of tuples (file_id, db_index)
            
        Returns:
            Dict with success/failure counts
        """
        results = {"deleted": 0, "failed": 0, "total": len(file_ids)}
        
        for file_id, db_index in file_ids:
            try:
                if await self.delete_file(file_id, db_index):
                    results["deleted"] += 1
                else:
                    results["failed"] += 1
            except Exception as e:
                LOGGER.error(f"[UnsortedDB] Bulk delete error for {file_id}: {e}")
                results["failed"] += 1
        
        return results
    
    async def list_files(
        self,
        page: int = 1,
        page_size: int = 24,
        search: str = "",
        media_type: str = "",
        sort_field: str = "created_on",
        sort_order: str = "desc"
    ) -> dict:
        """
        List unsorted files with pagination, search, and filtering.
        
        Args:
            page: Page number (1-indexed)
            page_size: Items per page (0 = return all)
            search: Search term for filename and caption
            media_type: Filter by media type (archive, audio, document, video, other)
            sort_field: Field to sort by
            sort_order: "asc" or "desc"
            
        Returns:
            Dict with files, pagination info
        """
        # Build filter
        filter_dict = {}
        
        if search:
            # Fuzzy search on filename and caption
            regex_query = {"$regex": search, "$options": "i"}
            filter_dict["$or"] = [
                {"file_name": regex_query},
                {"caption": regex_query}
            ]
        
        if media_type:
            filter_dict["media_type"] = media_type
        
        # Build sort
        sort_direction = DESCENDING if sort_order.lower() == "desc" else ASCENDING
        sort_dict = [(sort_field, sort_direction)]
        
        # Pagination
        skip = (page - 1) * page_size if page_size > 0 else 0
        
        results = []
        dbs_checked = []
        total_count = 0
        
        # Count total matching documents across all databases
        db_counts = []
        for i in range(1, self.current_db_index + 1):
            db_key = f"storage_{i}"
            collection = self.dbs[db_key][self.COLLECTION_NAME]
            count = await collection.count_documents(filter_dict)
            db_counts.append((i, count))
            total_count += count
        
        # Stream all if page_size is 0
        if page_size == 0:
            for db_index, _ in reversed(db_counts):
                db_key = f"storage_{db_index}"
                collection = self.dbs[db_key][self.COLLECTION_NAME]
                dbs_checked.append(db_index)
                
                cursor = collection.find(filter_dict).sort(sort_dict)
                async for doc in cursor:
                    results.append(convert_objectid_to_str(doc))
            
            return {
                "total_count": total_count,
                "total_pages": 1,
                "current_page": 1,
                "databases_checked": dbs_checked,
                "files": results
            }
        
        # Find starting database for pagination
        remaining_skip = skip
        start_db_index = None
        
        for db_index, count in reversed(db_counts):
            if remaining_skip < count:
                start_db_index = db_index
                break
            remaining_skip -= count
        
        if not start_db_index and total_count > 0:
            start_db_index = self.current_db_index
            remaining_skip = 0
        
        # Fetch paginated results
        if start_db_index:
            for db_index, count in reversed(db_counts):
                if db_index < start_db_index:
                    continue
                
                db_key = f"storage_{db_index}"
                collection = self.dbs[db_key][self.COLLECTION_NAME]
                dbs_checked.append(db_index)
                
                current_skip = remaining_skip if db_index == start_db_index else 0
                current_limit = page_size - len(results)
                
                if current_limit <= 0:
                    break
                
                cursor = (
                    collection.find(filter_dict)
                    .sort(sort_dict)
                    .skip(current_skip)
                    .limit(current_limit)
                )
                
                docs = await cursor.to_list(None)
                results.extend([convert_objectid_to_str(doc) for doc in docs])
                
                if len(results) >= page_size:
                    break
        
        total_pages = (total_count + page_size - 1) // page_size if page_size > 0 else 1
        
        return {
            "total_count": total_count,
            "total_pages": max(1, total_pages),
            "current_page": page,
            "databases_checked": dbs_checked,
            "files": results
        }
    
    async def get_stats(self) -> dict:
        """
        Get statistics about unsorted files.
        
        Returns:
            Dict with counts by media type, total size, etc.
        """
        stats = {
            "total_files": 0,
            "total_size_bytes": 0,
            "by_media_type": {},
            "by_database": []
        }
        
        try:
            for i in range(1, self.current_db_index + 1):
                db_key = f"storage_{i}"
                collection = self.dbs[db_key][self.COLLECTION_NAME]
                
                # Count per database
                count = await collection.count_documents({})
                stats["total_files"] += count
                
                # Aggregate by media type
                pipeline = [
                    {"$group": {
                        "_id": "$media_type",
                        "count": {"$sum": 1},
                        "total_size": {"$sum": "$size_bytes"}
                    }}
                ]
                
                async for doc in collection.aggregate(pipeline):
                    media_type = doc["_id"] or "other"
                    if media_type not in stats["by_media_type"]:
                        stats["by_media_type"][media_type] = {"count": 0, "size_bytes": 0}
                    stats["by_media_type"][media_type]["count"] += doc["count"]
                    stats["by_media_type"][media_type]["size_bytes"] += doc.get("total_size", 0)
                    stats["total_size_bytes"] += doc.get("total_size", 0)
                
                stats["by_database"].append({
                    "db_index": i,
                    "count": count
                })
                
        except Exception as e:
            LOGGER.error(f"[UnsortedDB] Stats failed: {e}")
        
        return stats
    
    async def get_all_telegram_ids(self) -> set:
        """
        Get all telegram message IDs in the unsorted collection.
        Used by scan_unsorted to check for existing entries.
        
        Returns:
            Set of tuples (chat_id, msg_id)
        """
        existing = set()
        
        try:
            for i in range(1, self.current_db_index + 1):
                db_key = f"storage_{i}"
                collection = self.dbs[db_key][self.COLLECTION_NAME]
                
                async for doc in collection.find({}, {"telegram_id": 1}):
                    try:
                        telegram_id = doc.get("telegram_id")
                        if telegram_id:
                            decoded = await decode_string(telegram_id)
                            existing.add((
                                str(decoded.get("chat_id")),
                                int(decoded.get("msg_id"))
                            ))
                    except Exception:
                        pass
                        
        except Exception as e:
            LOGGER.error(f"[UnsortedDB] Get telegram IDs failed: {e}")
        
        return existing

