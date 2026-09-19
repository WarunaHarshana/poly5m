"""Assemble H-006 midband bot from _bs.* shards; retire H-001 sniper."""
from __future__ import annotations

import base64
from pathlib import Path

_dir = Path(__file__).resolve().parent
_n = 68
_code = "".join(
    base64.b64decode((_dir / f"_bs.{i:02d}").read_text().strip()).decode("utf-8")
    for i in range(1, _n + 1)
)
exec(compile(_code, str(_dir / "_assembled_bot.py"), "exec"), globals())

try:
    import twap_engine as _te
    from twap_midband_patch import patch_twap_engine as _patch
    _patch(_te.TWAPEngine)
except Exception as _e:
    print("midband engine patch warn:", _e)

if __name__ == "__main__":
    main()
