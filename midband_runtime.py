"""H-006 midband runtime: patch TWAPBot (paper mid-band maker; H-001 retired)."""
from __future__ import annotations

import base64
import logging
import runpy
import zlib
from pathlib import Path

_log = logging.getLogger("midband_runtime")

_dir = Path(__file__).resolve().parent
for _i in (1, 2):
    runpy.run_path(str(_dir / f"_write_mr{_i}.py"), run_name="__main__")
_PAYLOAD = "".join(
    (_dir / f"_mr.{i}.b64").read_text(encoding="ascii").strip() for i in (1, 2)
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
