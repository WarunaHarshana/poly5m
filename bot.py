"""Polymarket 5m bot — MIDBAND_MAKER assembler (H-001 retired)."""
from __future__ import annotations
from pathlib import Path
_dir = Path(__file__).resolve().parent
_src = "".join((_dir / f"bot_part{i}.py").read_text(encoding="utf-8") for i in range(1, 5+1))
exec(compile(_src, str(_dir / "bot_joined.py"), "exec"), globals())
if __name__ == "__main__":
    main()
