"""Materialize stock-bot shards, inflate, install H-006 midband runtime."""
from __future__ import annotations

import base64
import runpy
import types
import zlib
from pathlib import Path

_dir = Path(__file__).resolve().parent
for _i in range(1, 6):
    runpy.run_path(str(_dir / f"_write_ob{_i}.py"), run_name="__main__")

_payload = "".join(
    (_dir / f"_ob.{i}.b64").read_text(encoding="ascii").strip() for i in range(1, 6)
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
