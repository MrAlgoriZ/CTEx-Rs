from typing import Any, Dict


def error(message: str | None = None) -> Dict:
    return {"success": False, "data": None, "message": message}


def success(data: Any, message: str | None = None) -> Dict:
    return {
        "success": True,
        "data": data,
        "message": message,
    }
