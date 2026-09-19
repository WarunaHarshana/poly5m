"""H-006 midband runtime (compat). evaluate_midband lives on TWAPEngine via twap_midband_patch."""
from __future__ import annotations
import logging
_log = logging.getLogger("midband_runtime")

def install(bot_module) -> None:
    """No-op: midband is compiled into bot.py / twap_engine.py."""
    if getattr(bot_module, "_midband_runtime_installed", False):
        return
    try:
        import twap_engine as te
        from twap_midband_patch import patch_twap_engine
        patch_twap_engine(te.TWAPEngine)
    except Exception as e:
        _log.warning("patch twap_engine failed: %s", e)
    bot_module._midband_runtime_installed = True
    _log.info("midband_runtime compat install (plain bot.py; H-001 retired)")
