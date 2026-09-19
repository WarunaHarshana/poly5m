"""Bot entrypoint: inflate compressed implementation (_bot_zlib_b64.txt)."""
from __future__ import annotations

import base64
import zlib
from pathlib import Path

_payload = Path(__file__).with_name("_bot_zlib_b64.txt").read_text(encoding="ascii").strip()
_code = zlib.decompress(base64.b64decode(_payload)).decode("utf-8")
exec(compile(_code, str(Path(__file__).with_name("bot_impl.py")), "exec"), globals())
