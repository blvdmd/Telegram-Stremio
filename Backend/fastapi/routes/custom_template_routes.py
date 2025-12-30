"""
Custom template routes for Play UI pages.
These routes are isolated from upstream changes to minimize merge conflicts.
Public access - no authentication required.
"""

from fastapi import Request, HTTPException
from fastapi.templating import Jinja2Templates
from Backend.fastapi.themes import get_theme, get_all_themes
from Backend.fastapi.security.credentials import is_authenticated, get_current_user
from Backend import db


templates = Jinja2Templates(directory="Backend/fastapi/templates")


def get_base_url(request: Request) -> str:
    """Extract base URL from the incoming request (scheme://host:port)"""
    return f"{request.url.scheme}://{request.url.netloc}"


async def play_browse_page(request: Request, media_type: str = "movie"):
    """
    Public browse page for streaming media.
    Similar to media_management but without edit/delete actions.
    """
    theme_name = request.session.get("theme", "purple_gradient")
    theme = get_theme(theme_name)
    
    # Check if user is authenticated (for showing nav items)
    current_user = get_current_user(request) if is_authenticated(request) else None
    
    return templates.TemplateResponse("play_browse.html", {
        "request": request,
        "theme": theme,
        "themes": get_all_themes(),
        "current_theme": theme_name,
        "current_user": current_user,
        "media_type": media_type
    })


async def play_files_page(request: Request, tmdb_id: int, db_index: int, media_type: str):
    """
    Public file listing page for a specific media item.
    Shows available files with download/streaming URLs.
    """
    theme_name = request.session.get("theme", "purple_gradient")
    theme = get_theme(theme_name)
    
    # Check if user is authenticated (for showing nav items)
    current_user = get_current_user(request) if is_authenticated(request) else None
    
    # Get base URL for constructing streaming URLs
    base_url = get_base_url(request)
    
    try:
        media_details = await db.get_document(media_type, tmdb_id, db_index)
        if not media_details:
            media_details = None
    except Exception as e:
        media_details = None
    
    return templates.TemplateResponse("play_files.html", {
        "request": request,
        "theme": theme,
        "themes": get_all_themes(),
        "current_theme": theme_name,
        "current_user": current_user,
        "tmdb_id": tmdb_id,
        "db_index": db_index,
        "media_type": media_type,
        "media_details": media_details,
        "base_url": base_url
    })

