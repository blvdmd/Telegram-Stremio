"""
API routes for unsorted files.
Isolated from main routes to avoid merge conflicts with upstream.
"""

from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Tuple

from Backend import unsorted_collection
from Backend.logger import LOGGER


router = APIRouter(prefix="/api/files_unsorted", tags=["Unsorted Files API"])


# ===== Request Models =====

class BulkDeleteRequest(BaseModel):
    """Request body for bulk delete operation."""
    files: List[dict]  # List of {"id": str, "db_index": int}


# ===== Helper Functions =====

def get_base_url(request: Request) -> str:
    """Extract base URL from the incoming request."""
    return f"{request.url.scheme}://{request.url.netloc}"


def sanitize_filename(filename: str, max_bytes: int = 250) -> str:
    """
    Truncate filename to max_bytes while preserving the file extension.
    Uses byte length (not character count) for SMB/Samba compatibility
    since Unicode characters can take multiple bytes.
    """
    if not filename:
        return filename
    
    encoded = filename.encode('utf-8')
    if len(encoded) <= max_bytes:
        return filename
    
    # Extract extension (e.g., .mkv)
    dot_idx = filename.rfind('.')
    if dot_idx > 0:
        ext = filename[dot_idx:]
        base = filename[:dot_idx]
        ext_bytes = len(ext.encode('utf-8'))
    else:
        ext = ""
        base = filename
        ext_bytes = 0
    
    # Truncate base to fit within limit
    target_base_bytes = max_bytes - ext_bytes
    if target_base_bytes <= 0:
        # Extension alone exceeds limit, just truncate everything
        return filename.encode('utf-8')[:max_bytes].decode('utf-8', errors='ignore')
    
    base_encoded = base.encode('utf-8')[:target_base_bytes]
    # Decode safely, ignoring partial UTF-8 sequences at the truncation point
    truncated_base = base_encoded.decode('utf-8', errors='ignore')
    
    return truncated_base + ext


def enrich_file_with_url(file: dict, base_url: str) -> dict:
    """Add streaming_url to file document with sanitized filename."""
    if file and "telegram_id" in file:
        file_name = sanitize_filename(file.get("file_name", "file"))
        file["file_name"] = file_name
        file["streaming_url"] = f"{base_url}/dl/{file['telegram_id']}/{file_name}"
    return file


# ===== API Endpoints =====

@router.get("")
async def list_unsorted_files(
    request: Request,
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(25, ge=0, le=200, description="Items per page (0 = all)"),
    search: str = Query("", max_length=200, description="Search filename and caption"),
    media_type: str = Query("", description="Filter: archive, audio, document, video, other"),
    sort: str = Query("created_on", description="Sort field: created_on, file_name, size_bytes, media_type"),
    order: str = Query("desc", description="Sort order: asc, desc")
):
    """
    List unsorted files with pagination, search, and filtering.
    
    Query Parameters:
    - page: Page number (default: 1)
    - page_size: Items per page, 0 for all (default: 25, max: 200)
    - search: Search in filename and caption
    - media_type: Filter by type (archive, audio, document, video, other)
    - sort: Sort by field (created_on, file_name, size_bytes, media_type)
    - order: Sort direction (asc, desc)
    """
    try:
        base_url = get_base_url(request)
        
        # Validate sort field
        valid_sort_fields = ["created_on", "file_name", "size_bytes", "media_type", "updated_on"]
        if sort not in valid_sort_fields:
            sort = "created_on"
        
        # Validate order
        if order.lower() not in ["asc", "desc"]:
            order = "desc"
        
        # Validate media_type
        valid_media_types = ["archive", "audio", "document", "video", "other", ""]
        if media_type not in valid_media_types:
            media_type = ""
        
        # Fetch files
        result = await unsorted_collection.list_files(
            page=page,
            page_size=page_size,
            search=search,
            media_type=media_type,
            sort_field=sort,
            sort_order=order
        )
        
        # Enrich files with streaming URLs
        result["files"] = [
            enrich_file_with_url(f, base_url) 
            for f in result.get("files", [])
        ]
        
        return result
        
    except Exception as e:
        LOGGER.error(f"[UnsortedAPI] List failed: {e}")
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )


@router.get("/stats")
async def get_stats():
    """
    Get statistics about unsorted files.
    
    Returns counts by media type and total storage used.
    """
    try:
        stats = await unsorted_collection.get_stats()
        return stats
    except Exception as e:
        LOGGER.error(f"[UnsortedAPI] Stats failed: {e}")
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )


@router.get("/{file_id}")
async def get_file(request: Request, file_id: str, db_index: int = Query(...)):
    """
    Get a single file by ID.
    
    Path Parameters:
    - file_id: MongoDB ObjectId
    
    Query Parameters:
    - db_index: Database index where file is stored
    """
    try:
        base_url = get_base_url(request)
        
        file = await unsorted_collection.get_file(file_id, db_index)
        if not file:
            return JSONResponse(
                status_code=404,
                content={"detail": "File not found"}
            )
        
        file = enrich_file_with_url(file, base_url)
        return file
        
    except Exception as e:
        LOGGER.error(f"[UnsortedAPI] Get file failed: {e}")
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )


@router.delete("/{file_id}")
async def delete_file(file_id: str, db_index: int = Query(...)):
    """
    Delete a single file from database AND Telegram channel.
    
    Path Parameters:
    - file_id: MongoDB ObjectId
    
    Query Parameters:
    - db_index: Database index where file is stored
    """
    try:
        success = await unsorted_collection.delete_file(file_id, db_index)
        
        if success:
            return {"success": True, "message": "File deleted successfully"}
        else:
            return JSONResponse(
                status_code=404,
                content={"success": False, "detail": "File not found or delete failed"}
            )
            
    except Exception as e:
        LOGGER.error(f"[UnsortedAPI] Delete failed: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "detail": str(e)}
        )


@router.post("/bulk-delete")
async def delete_files_bulk(request_body: BulkDeleteRequest):
    """
    Delete multiple files from database AND Telegram channel.
    
    Request Body:
    - files: List of {"id": str, "db_index": int}
    """
    try:
        # Convert to list of tuples
        file_ids: List[Tuple[str, int]] = [
            (f["id"], f["db_index"]) 
            for f in request_body.files
        ]
        
        if not file_ids:
            return JSONResponse(
                status_code=400,
                content={"success": False, "detail": "No files specified"}
            )
        
        result = await unsorted_collection.delete_files_bulk(file_ids)
        
        return {
            "success": True,
            "deleted": result["deleted"],
            "failed": result["failed"],
            "total": result["total"]
        }
        
    except Exception as e:
        LOGGER.error(f"[UnsortedAPI] Bulk delete failed: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "detail": str(e)}
        )

