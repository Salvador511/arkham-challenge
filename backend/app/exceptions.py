"""
DEPRECATED FILE: All exceptions have been moved to app.core.exceptions

This file is kept only for backward compatibility.

Update your imports:
    # Old (deprecated)
    from app.exceptions import ...

    # New (correct)
    from app.core.exceptions import ...

All exceptions are now in a centralized location at:
    app/core/exceptions.py
"""

# Re-export from new location for backward compatibility
from app.core.exceptions import *  # noqa: F401, F403
