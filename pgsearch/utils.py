import re
from pathlib import Path

_SAKSNR_PATTERN = re.compile(r"^(\d{2})_(\d{3,6})$")


def extract_saksnr(path: str | Path) -> str | None:
    """Extract saksnr (e.g. '24/1234') from a path containing a directory like '24_1234'."""
    for part in Path(path).parts:
        m = _SAKSNR_PATTERN.match(part)
        if m:
            return f"{m.group(1)}/{m.group(2)}"
    return None
