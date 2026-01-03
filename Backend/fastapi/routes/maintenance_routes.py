"""
Maintenance API routes for tidy and scan operations.
These endpoints allow triggering maintenance operations from the web UI.
"""

import asyncio
from datetime import datetime, timedelta
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List

from Backend.pyrofork.bot import StreamBot
from Backend.helper.maintenance import (
    run_tidy,
    run_scan,
    get_current_operation,
    is_operation_running,
    request_cancel,
    get_auth_channels,
    reset_operation,
)
from Backend.logger import LOGGER

router = APIRouter(prefix="/api/maintenance", tags=["Maintenance"])


# -------------------------------
# Request Models
# -------------------------------
class ScanRequest(BaseModel):
    channels: Optional[List[str]] = None  # None means all channels
    mode: str = "date"  # "date" or "count"
    date_preset: Optional[str] = None  # "24h", "7d", "30d", "90d", "all", "custom"
    start_date: Optional[str] = None  # For custom date range (YYYY-MM-DD)
    end_date: Optional[str] = None  # For custom date range (YYYY-MM-DD)
    limit: Optional[int] = None  # For count mode


# -------------------------------
# Helper Functions
# -------------------------------
def parse_date_preset(preset: str) -> tuple:
    """Parse date preset into start_date and end_date (local time to match Pyrogram's message.date)"""
    # Use local time to match Pyrogram's message.date conversion
    end_date = datetime.now()
    
    if preset == "24h":
        start_date = end_date - timedelta(hours=24)
    elif preset == "7d":
        start_date = end_date - timedelta(days=7)
    elif preset == "30d":
        start_date = end_date - timedelta(days=30)
    elif preset == "90d":
        start_date = end_date - timedelta(days=90)
    elif preset == "all":
        start_date = None
        end_date = None
    else:
        # Default to 7 days
        start_date = end_date - timedelta(days=7)
    
    return start_date, end_date


def parse_custom_date(date_str: str) -> Optional[datetime]:
    """Parse a date string (YYYY-MM-DD) to datetime"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d")
    except ValueError:
        return None


async def run_tidy_background():
    """Background task to run tidy operation"""
    try:
        await run_tidy(StreamBot)
    except Exception as e:
        LOGGER.error(f"[Maintenance API] Tidy error: {e}")


async def run_scan_background(channels, mode, start_date, end_date, limit, mode_label=None):
    """Background task to run scan operation"""
    try:
        await run_scan(StreamBot, channels, mode, start_date, end_date, limit, mode_label)
    except Exception as e:
        LOGGER.error(f"[Maintenance API] Scan error: {e}")


# -------------------------------
# API Endpoints
# -------------------------------
@router.get("/channels")
async def get_channels():
    """Get list of configured AUTH_CHANNELs with names for scan operation"""
    from Backend.helper.maintenance import normalize_channel_id
    
    channel_ids = get_auth_channels()
    channels = []
    
    for ch_id in channel_ids:
        channel_info = {"id": str(ch_id), "name": str(ch_id)}
        
        # Try to get channel name if bot is connected
        if StreamBot.is_connected:
            try:
                chat_id = normalize_channel_id(ch_id)
                chat = await StreamBot.get_chat(chat_id)
                channel_info["name"] = chat.title or str(ch_id)
            except Exception:
                pass  # Keep ID as name if lookup fails
        
        channels.append(channel_info)
    
    return {
        "channels": channels,
        "count": len(channels)
    }


@router.get("/status")
async def get_status():
    """Get current operation status"""
    operation = get_current_operation()
    if operation:
        return operation.to_dict()
    return {
        "operation_type": None,
        "status": "idle"
    }


@router.get("/status/stream")
async def stream_status():
    """Stream operation status via Server-Sent Events"""
    async def event_generator():
        import json
        
        while True:
            operation = get_current_operation()
            if operation:
                data = json.dumps(operation.to_dict())
                yield f"data: {data}\n\n"
                
                # Stop streaming when operation is complete
                if operation.status in ("completed", "cancelled", "error"):
                    break
            else:
                yield f"data: {json.dumps({'status': 'idle'})}\n\n"
                break
            
            await asyncio.sleep(1)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@router.post("/tidy")
async def start_tidy():
    """Start tidy operation"""
    if is_operation_running():
        return {
            "success": False,
            "error": "Another operation is already running"
        }
    
    # Check if bot is connected
    if not StreamBot.is_connected:
        return {
            "success": False,
            "error": "Bot is not connected. Please wait for bot to start."
        }
    
    # Reset any previous state
    reset_operation()
    
    # Start operation as async task (faster than BackgroundTasks thread pool)
    asyncio.create_task(run_tidy_background())
    
    return {
        "success": True,
        "message": "Tidy operation started"
    }


@router.post("/scan")
async def start_scan(request: ScanRequest):
    """Start scan operation"""
    if is_operation_running():
        return {
            "success": False,
            "error": "Another operation is already running"
        }
    
    # Check if bot is connected
    if not StreamBot.is_connected:
        return {
            "success": False,
            "error": "Bot is not connected. Please wait for bot to start."
        }
    
    # Reset any previous state
    reset_operation()
    
    # Determine channels to scan
    all_channels = get_auth_channels()
    if not all_channels:
        return {
            "success": False,
            "error": "No AUTH_CHANNELs configured"
        }
    
    if request.channels:
        channels = request.channels
    else:
        channels = all_channels
    
    # Parse date/count settings
    start_date = None
    end_date = None
    limit = None
    mode_label = None
    
    # Preset to label mapping (matches bot labels)
    preset_labels = {
        "24h": "Last 24 hours",
        "7d": "Last 7 days",
        "30d": "Last 30 days",
        "90d": "Last 90 days",
        "all": "All time",
    }
    
    if request.mode == "date":
        if request.date_preset == "custom":
            # Custom date range
            start_date = parse_custom_date(request.start_date)
            end_date = parse_custom_date(request.end_date)
            # Set end_date to end of day for inclusive search
            if end_date:
                end_date = end_date.replace(hour=23, minute=59, second=59)
            if not start_date:
                return {
                    "success": False,
                    "error": "Start date is required for custom date range"
                }
            # Custom label with date range
            start_str = start_date.strftime('%b %d, %Y') if start_date else "?"
            end_str = end_date.strftime('%b %d, %Y') if end_date else "now"
            mode_label = f"{start_str} → {end_str}"
        else:
            start_date, end_date = parse_date_preset(request.date_preset or "7d")
            mode_label = preset_labels.get(request.date_preset, "Last 7 days")
    else:
        limit = request.limit or 100
        mode_label = f"Last {limit} messages"
    
    # Start operation as async task (faster than BackgroundTasks thread pool)
    asyncio.create_task(
        run_scan_background(channels, request.mode, start_date, end_date, limit, mode_label)
    )
    
    return {
        "success": True,
        "message": "Scan operation started"
    }


@router.post("/cancel")
async def cancel_operation():
    """Cancel current operation"""
    if not is_operation_running():
        return {
            "success": False,
            "error": "No operation is running"
        }
    
    request_cancel()
    
    return {
        "success": True,
        "message": "Cancel requested"
    }


@router.post("/reset")
async def reset():
    """Reset operation state (use if stuck)"""
    reset_operation()
    return {
        "success": True,
        "message": "Operation state reset"
    }

