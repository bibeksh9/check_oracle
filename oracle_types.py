from typing import Any, Dict

# Type alias for row results
DictRow = Dict[str, Any]

def dict_row(cursor, row):
    """Convert a row to a dictionary"""
    if row is None:
        return None
    return {d[0]: v for d, v in zip(cursor.description, row)}

class Capabilities:
    """Oracle capabilities checker"""
    def has_pipeline(self) -> bool:
        return True

class Jsonb:
    """Oracle JSON wrapper"""
    def __init__(self, data: Any):
        self.data = data

    def __str__(self):
        return str(self.data)

# Export these for use elsewhere
__all__ = ['DictRow', 'dict_row', 'Capabilities', 'Jsonb']
