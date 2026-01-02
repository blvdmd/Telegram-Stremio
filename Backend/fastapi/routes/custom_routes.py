"""
Custom API routes for extended media listing functionality.
These routes are isolated from upstream changes to minimize merge conflicts.
"""

import orjson
from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse, StreamingResponse
from Backend import db
from Backend.helper.database import convert_objectid_to_str
from typing import AsyncGenerator
from pymongo import DESCENDING

router = APIRouter(tags=["Custom Media API"])


def get_base_url(request: Request) -> str:
    """Extract base URL from the incoming request (scheme://host:port)"""
    return f"{request.url.scheme}://{request.url.netloc}"


def enrich_telegram_object_inplace(telegram_item: dict, base_url: str) -> None:
    """
    Enrich a telegram object with streaming_url IN PLACE (no copy).
    Modifies the dict directly for better memory efficiency.
    """
    telegram_id = telegram_item.get("id", "")
    telegram_item["streaming_url"] = f"{base_url}/dl/{telegram_id}/video.mkv"
    
    # Set defaults only if not present
    telegram_item.setdefault("sizeInBytes", None)
    telegram_item.setdefault("updated_on", None)
    telegram_item.setdefault("created_on", None)


def enrich_movie_inplace(movie: dict, base_url: str) -> dict:
    """
    Enrich all telegram objects in a movie IN PLACE.
    Returns the same dict (modified) for chaining.
    """
    if "telegram" in movie and movie["telegram"]:
        for t in movie["telegram"]:
            enrich_telegram_object_inplace(t, base_url)
    return movie


def enrich_tv_show_inplace(tv_show: dict, base_url: str) -> dict:
    """
    Enrich all telegram objects in a TV show IN PLACE.
    Returns the same dict (modified) for chaining.
    """
    if "seasons" in tv_show:
        for season in tv_show["seasons"]:
            if "episodes" in season:
                for episode in season["episodes"]:
                    if "telegram" in episode and episode["telegram"]:
                        for t in episode["telegram"]:
                            enrich_telegram_object_inplace(t, base_url)
    return tv_show


def build_filter(search: str = "", genre: str = "") -> dict:
    """Build MongoDB filter dict from search and genre parameters."""
    filter_dict = {}
    if search:
        filter_dict["title"] = {"$regex": search, "$options": "i"}
    if genre:
        filter_dict["genres"] = {"$in": [genre]}
    return filter_dict


async def stream_movies_json(base_url: str, search: str = "", genre: str = "") -> AsyncGenerator[str, None]:
    """
    Async generator that streams movies as JSON array.
    Memory efficient - processes documents one at a time using cursor batching.
    """
    filter_dict = build_filter(search, genre)
    
    yield '{"movies":['
    first = True
    
    for i in range(1, db.current_db_index + 1):
        db_key = f"storage_{i}"
        collection = db.dbs[db_key]["movie"]
        
        async for doc in collection.find(filter_dict).batch_size(100):
            if not first:
                yield ','
            first = False
            # Convert ObjectId and enrich in place - no deep copies
            converted = convert_objectid_to_str(doc)
            enrich_movie_inplace(converted, base_url)
            yield orjson.dumps(converted).decode()
    
    yield ']}'


async def stream_tv_shows_json(base_url: str, search: str = "", genre: str = "") -> AsyncGenerator[str, None]:
    """
    Async generator that streams TV shows as JSON array.
    Memory efficient - processes documents one at a time using cursor batching.
    """
    filter_dict = build_filter(search, genre)
    
    yield '{"tv_shows":['
    first = True
    
    for i in range(1, db.current_db_index + 1):
        db_key = f"storage_{i}"
        collection = db.dbs[db_key]["tv"]
        
        async for doc in collection.find(filter_dict).batch_size(100):
            if not first:
                yield ','
            first = False
            converted = convert_objectid_to_str(doc)
            enrich_tv_show_inplace(converted, base_url)
            yield orjson.dumps(converted).decode()
    
    yield ']}'


async def stream_all_media_json(base_url: str, search: str = "", genre: str = "") -> AsyncGenerator[str, None]:
    """
    Async generator that streams both movies and TV shows as JSON.
    Memory efficient - processes documents one at a time using cursor batching.
    """
    filter_dict = build_filter(search, genre)
    
    yield '{"movies":['
    first = True
    
    # Stream movies
    for i in range(1, db.current_db_index + 1):
        db_key = f"storage_{i}"
        collection = db.dbs[db_key]["movie"]
        
        async for doc in collection.find(filter_dict).batch_size(100):
            if not first:
                yield ','
            first = False
            converted = convert_objectid_to_str(doc)
            enrich_movie_inplace(converted, base_url)
            yield orjson.dumps(converted).decode()
    
    yield '],"tv_shows":['
    first = True
    
    # Stream TV shows
    for i in range(1, db.current_db_index + 1):
        db_key = f"storage_{i}"
        collection = db.dbs[db_key]["tv"]
        
        async for doc in collection.find(filter_dict).batch_size(100):
            if not first:
                yield ','
            first = False
            converted = convert_objectid_to_str(doc)
            enrich_tv_show_inplace(converted, base_url)
            yield orjson.dumps(converted).decode()
    
    yield ']}'


async def paginate_collection_with_search(
    collection_name: str,
    page: int,
    page_size: int,
    search: str = "",
    genre: str = ""
) -> dict:
    """
    Paginate a collection with proper server-side search filtering.
    This ensures search is applied BEFORE pagination for correct results.
    
    Returns dict with: items, total_count, total_pages, current_page, databases_checked
    """
    filter_dict = build_filter(search, genre)
    skip = (page - 1) * page_size
    
    results = []
    dbs_checked = []
    total_count = 0
    
    # Count total matching documents across all databases
    db_counts = []
    for i in range(1, db.current_db_index + 1):
        db_key = f"storage_{i}"
        collection = db.dbs[db_key][collection_name]
        count = await collection.count_documents(filter_dict)
        db_counts.append((i, count))
        total_count += count
    
    # Find starting database for pagination
    remaining_skip = skip
    start_db_index = None
    
    for db_index, count in reversed(db_counts):
        if remaining_skip < count:
            start_db_index = db_index
            break
        remaining_skip -= count
    
    if not start_db_index and total_count > 0:
        # Edge case: skip exceeds total, return empty
        start_db_index = db.current_db_index
        remaining_skip = 0
    
    # Fetch paginated results
    if start_db_index:
        for db_index, count in reversed(db_counts):
            if db_index < start_db_index:
                continue
            
            db_key = f"storage_{db_index}"
            collection = db.dbs[db_key][collection_name]
            dbs_checked.append(db_index)
            
            current_skip = remaining_skip if db_index == start_db_index else 0
            current_limit = page_size - len(results)
            
            if current_limit <= 0:
                break
            
            cursor = (
                collection.find(filter_dict)
                .sort("updated_on", DESCENDING)
                .skip(current_skip)
                .limit(current_limit)
            )
            
            docs = await cursor.to_list(None)
            results.extend(docs)
            
            if len(results) >= page_size:
                break
    
    total_pages = (total_count + page_size - 1) // page_size if page_size > 0 else 1
    
    return {
        "items": results,
        "total_count": total_count,
        "total_pages": max(1, total_pages),
        "current_page": page,
        "databases_checked": dbs_checked
    }


@router.get("/api/media/listall")
async def list_all_media(
    request: Request,
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(24, ge=0, description="Items per page (0 = stream all records)"),
    search: str = Query("", max_length=100, description="Search by title"),
    genre: str = Query("", max_length=50, description="Filter by genre")
):
    """
    Get combined list of movies and TV shows with enriched telegram metadata.
    
    - page_size=0: Streams ALL records efficiently (memory-safe)
    - page_size>0: Returns paginated results with server-side search
    """
    try:
        base_url = get_base_url(request)
        
        # Stream all records when page_size=0
        if page_size == 0:
            return StreamingResponse(
                stream_all_media_json(base_url, search, genre),
                media_type="application/json"
            )
        
        # Fetch movies and TV shows with proper server-side search
        movies_data = await paginate_collection_with_search("movie", page, page_size, search, genre)
        tv_data = await paginate_collection_with_search("tv", page, page_size, search, genre)
        
        # Enrich results in place (no deep copies)
        enriched_movies = [
            enrich_movie_inplace(convert_objectid_to_str(m), base_url)
            for m in movies_data["items"]
        ]
        enriched_tv_shows = [
            enrich_tv_show_inplace(convert_objectid_to_str(t), base_url)
            for t in tv_data["items"]
        ]
        
        # Combine databases checked
        dbs_checked = list(set(
            movies_data["databases_checked"] + tv_data["databases_checked"]
        ))
        
        return {
            "total_count": movies_data["total_count"] + tv_data["total_count"],
            "total_pages": max(movies_data["total_pages"], tv_data["total_pages"]),
            "databases_checked": dbs_checked,
            "current_page": page,
            "movies": enriched_movies,
            "tv_shows": enriched_tv_shows
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.get("/api/media/list_movie")
async def list_movies(
    request: Request,
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(24, ge=0, description="Items per page (0 = stream all records)"),
    search: str = Query("", max_length=100, description="Search by title"),
    genre: str = Query("", max_length=50, description="Filter by genre")
):
    """
    Get list of movies with enriched telegram metadata.
    
    - page_size=0: Streams ALL records efficiently (memory-safe)
    - page_size>0: Returns paginated results with server-side search
    """
    try:
        base_url = get_base_url(request)
        
        # Stream all records when page_size=0
        if page_size == 0:
            return StreamingResponse(
                stream_movies_json(base_url, search, genre),
                media_type="application/json"
            )
        
        # Fetch movies with proper server-side search
        movies_data = await paginate_collection_with_search("movie", page, page_size, search, genre)
        
        # Enrich results in place
        enriched_movies = [
            enrich_movie_inplace(convert_objectid_to_str(m), base_url)
            for m in movies_data["items"]
        ]
        
        return {
            "total_count": movies_data["total_count"],
            "total_pages": movies_data["total_pages"],
            "databases_checked": movies_data["databases_checked"],
            "current_page": movies_data["current_page"],
            "movies": enriched_movies
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.get("/api/media/list_tv")
async def list_tv_shows(
    request: Request,
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(24, ge=0, description="Items per page (0 = stream all records)"),
    search: str = Query("", max_length=100, description="Search by title"),
    genre: str = Query("", max_length=50, description="Filter by genre")
):
    """
    Get list of TV shows with enriched telegram metadata.
    
    - page_size=0: Streams ALL records efficiently (memory-safe)
    - page_size>0: Returns paginated results with server-side search
    """
    try:
        base_url = get_base_url(request)
        
        # Stream all records when page_size=0
        if page_size == 0:
            return StreamingResponse(
                stream_tv_shows_json(base_url, search, genre),
                media_type="application/json"
            )
        
        # Fetch TV shows with proper server-side search
        tv_data = await paginate_collection_with_search("tv", page, page_size, search, genre)
        
        # Enrich results in place
        enriched_tv_shows = [
            enrich_tv_show_inplace(convert_objectid_to_str(t), base_url)
            for t in tv_data["items"]
        ]
        
        return {
            "total_count": tv_data["total_count"],
            "total_pages": tv_data["total_pages"],
            "databases_checked": tv_data["databases_checked"],
            "current_page": tv_data["current_page"],
            "tv_shows": enriched_tv_shows
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})
