"""Inflate stock bot, then install H-006 midband runtime (paper mid-band maker)."""
from __future__ import annotations

import base64
import types
import zlib
from pathlib import Path

_dir = Path(__file__).resolve().parent
_payload = "".join(
    p.read_text(encoding="ascii").strip() for p in sorted(_dir.glob("_ob.*.b64"))
)
_code = zlib.decompress(base64.b64decode(_payload)).decode("utf-8")
exec(compile(_code, str(_dir / "_stock_bot.py"), "exec"), globals())

import midband_runtime as _mr

_mod = types.ModuleType("bot")
for _k, _v in list(globals().items()):
    if _k.startswith("_") and _k not in ("_mr",):
        continue
    setattr(_mod, _k, _v)
for _k in ("TWAPBot", "TWAPEngine", "CoinSlot", "main"):
    if _k in globals():
        setattr(_mod, _k, globals()[_k])
_mr.install(_mod)
TWAPBot = _mod.TWAPBot

if __name__ == "__main__":
    main()
