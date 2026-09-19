"""Decode shard writers from adjacent *.b64txt files."""
from pathlib import Path
import base64
_dir = Path(__file__).resolve().parent
NAMES = [
    "_write_ob1_1.py","_write_ob1_2.py","_write_ob2_1.py","_write_ob2_2.py",
    "_write_ob3_1.py","_write_ob3_2.py","_write_ob4_1.py","_write_ob4_2.py",
    "_write_ob5_1.py","_write_ob5_2.py",
    "_write_mr1_1.py","_write_mr1_2.py","_write_mr2_1.py","_write_mr2_2.py",
]
for name in NAMES:
    blob = (_dir / f"{name}.b64txt").read_text().split()
    (_dir / name).write_bytes(base64.b64decode("".join(blob)))
print("decoded", len(NAMES), "shard writers")
