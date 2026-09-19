"""H-006 midband runtime: patch TWAPBot (paper mid-band maker; H-001 retired)."""
from __future__ import annotations

import base64
import logging
import zlib
from pathlib import Path

_log = logging.getLogger("midband_runtime")

_dir = Path(__file__).resolve().parent
_PAYLOAD = "".join(
    "".join((_dir / f"_methods.{i}.b64").read_text(encoding="ascii").split())
    for i in range(1, 5)
)


def install(bot_module) -> None:
    if getattr(bot_module, "_midband_runtime_installed", False):
        return
    try:
        import twap_engine as te
        from twap_midband_patch import patch_twap_engine
        patch_twap_engine(te.TWAPEngine)
    except Exception as e:
        _log.warning("patch twap_engine failed: %s", e)

    code = zlib.decompress(base64.b64decode(_PAYLOAD)).decode("utf-8")
    ns = dict(bot_module.__dict__)
    ns["__name__"] = "midband_methods"
    exec(compile(code, "midband_methods.py", "exec"), ns)
    bot_module.TWAPBot._tick_asset = ns["_tick_asset"]
    bot_module.TWAPBot.start = ns["start"]
    bot_module._midband_runtime_installed = True
    _log.info("midband_runtime installed (STRATEGY=MIDBAND_MAKER; H-001 retired)")
