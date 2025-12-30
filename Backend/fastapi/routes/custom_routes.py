"""
Custom API routes for extended media listing functionality.
These routes are isolated from upstream changes to minimize merge conflicts.
"""

from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse
from Backend import db
from typing import Optional
import copy

router = APIRouter(tags=["Custom Media API"])


def get_base_url(request: Request) -> str:
    """Extract base URL from the incoming request (scheme://host:port)"""
    return f"{request.url.scheme}://{request.url.netloc}"


def enrich_telegram_object(telegram_item: dict, base_url: str) -> dict:
    """
    Enrich a telegram object with streaming_url.
    Also includes sizeInBytes, updated_on, created_on if available in database.
    """
    enriched = copy.deepcopy(telegram_item)
    telegram_id = enriched.get("id", "")
    
    # Add streaming URL based on request's base URL
    enriched["streaming_url"] = f"{base_url}/dl/{telegram_id}/video.mkv"
    
    # These fields will be populated if they exist in the database
    # (added by receiver.py for new entries, or backfill script for existing ones)
    if "sizeInBytes" not in enriched:
        enriched["sizeInBytes"] = None
    if "updated_on" not in enriched:
        enriched["updated_on"] = None
    if "created_on" not in enriched:
        enriched["created_on"] = None
    
    return enriched


def enrich_movie(movie: dict, base_url: str) -> dict:
    """Enrich all telegram objects in a movie"""
    enriched_movie = copy.deepcopy(movie)
    if "telegram" in enriched_movie and enriched_movie["telegram"]:
        enriched_movie["telegram"] = [
            enrich_telegram_object(t, base_url) for t in enriched_movie["telegram"]
        ]
    return enriched_movie


def enrich_tv_show(tv_show: dict, base_url: str) -> dict:
    """Enrich all telegram objects in a TV show (nested in seasons/episodes)"""
    enriched_tv = copy.deepcopy(tv_show)
    if "seasons" in enriched_tv:
        for season in enriched_tv["seasons"]:
            if "episodes" in season:
                for episode in season["episodes"]:
                    if "telegram" in episode and episode["telegram"]:
                        episode["telegram"] = [
                            enrich_telegram_object(t, base_url) for t in episode["telegram"]
                        ]
    return enriched_tv


@router.get("/api/media/listall")
async def list_all_media(
    request: Request,
    count: int = Query(24, ge=1, le=500, description="Number of records to return")
):
    """
    Get combined list of movies and TV shows with enriched telegram metadata.
    Returns both media types in a single response.
    """
    try:
        base_url = get_base_url(request)
        
        # Fetch movies and TV shows
        # Using page=1 and page_size=count to get the requested number of items
        movies_result = await db.sort_movies([], page=1, page_size=count)
        tv_result = await db.sort_tv_shows([], page=1, page_size=count)
        
        # Enrich telegram objects with streaming URLs and metadata
        enriched_movies = [
            enrich_movie(movie, base_url) for movie in movies_result.get("movies", [])
        ]
        enriched_tv_shows = [
            enrich_tv_show(tv, base_url) for tv in tv_result.get("tv_shows", [])
        ]
        
        # Combine databases checked from both results
        dbs_checked = list(set(
            movies_result.get("databases_checked", []) + 
            tv_result.get("databases_checked", [])
        ))
        
        return {
            "total_count": movies_result.get("total_count", 0) + tv_result.get("total_count", 0),
            "total_pages": max(movies_result.get("total_pages", 1), tv_result.get("total_pages", 1)),
            "databases_checked": dbs_checked,
            "current_page": 1,
            "movies": enriched_movies,
            "tv_shows": enriched_tv_shows
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.get("/api/media/list_movie")
async def list_movies(
    request: Request,
    count: int = Query(24, ge=1, le=500, description="Number of records to return")
):
    """
    Get list of movies with enriched telegram metadata.
    """
    try:
        base_url = get_base_url(request)
        
        # Fetch movies
        movies_result = await db.sort_movies([], page=1, page_size=count)
        
        # Enrich telegram objects with streaming URLs and metadata
        enriched_movies = [
            enrich_movie(movie, base_url) for movie in movies_result.get("movies", [])
        ]
        
        return {
            "total_count": movies_result.get("total_count", 0),
            "total_pages": movies_result.get("total_pages", 1),
            "databases_checked": movies_result.get("databases_checked", []),
            "current_page": 1,
            "movies": enriched_movies
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.get("/api/media/list_tv")
async def list_tv_shows(
    request: Request,
    count: int = Query(24, ge=1, le=500, description="Number of records to return")
):
    """
    Get list of TV shows with enriched telegram metadata.
    """
    try:
        base_url = get_base_url(request)
        
        # Fetch TV shows
        tv_result = await db.sort_tv_shows([], page=1, page_size=count)
        
        # Enrich telegram objects with streaming URLs and metadata
        enriched_tv_shows = [
            enrich_tv_show(tv, base_url) for tv in tv_result.get("tv_shows", [])
        ]
        
        return {
            "total_count": tv_result.get("total_count", 0),
            "total_pages": tv_result.get("total_pages", 1),
            "databases_checked": tv_result.get("databases_checked", []),
            "current_page": 1,
            "tv_shows": enriched_tv_shows
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})

