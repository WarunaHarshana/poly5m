"""Compat stub — midband is compiled into bot.py / twap_engine.py."""
def install(bot_module) -> None:
    setattr(bot_module, "_midband_runtime_installed", True)
