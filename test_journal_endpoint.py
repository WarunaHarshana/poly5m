"""GET /journal: read-only, filtered, redacted, never crashes."""
import json, tempfile, threading, urllib.request
from pathlib import Path
from http.server import ThreadingHTTPServer

import http_api


def _write(p, recs):
    with open(p, "w") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
        f.write("not json\n")


def test_read_filters_and_limit(tmp_path):
    p = tmp_path / "j.jsonl"
    _write(p, [{"ts": i, "event": "maker_rest_tick" if i % 2 else "settlement", "i": i} for i in range(10)])
    rows = http_api.read_journal(p, since=4, events={"maker_rest_tick"}, limit=2)
    assert [r["i"] for r in rows] == [7, 9]  # newest-last
    assert len(http_api.read_journal(p)) == 10
    assert http_api.read_journal(tmp_path / "missing.jsonl") == []
    assert http_api.read_journal(None) == []
    assert len(http_api.read_journal(p, limit=10**9)) == 10


def test_redaction():
    r = http_api.redact({"event": "x", "order_id": "0x" + "a" * 64, "api_key": "k", "nested": {"secret": "s",
                         "signature": "z", "ok": 1, "blob": "0x" + "b" * 130}, "privateKey": "p", "token_id": "123"})
    assert r == {"event": "x", "order_id": "0x" + "a" * 64, "nested": {"ok": 1, "blob": "[redacted]"}, "token_id": "123"}


def _serve(handler):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def test_endpoint_ndjson_and_errors(tmp_path):
    p = tmp_path / "j.jsonl"
    _write(p, [{"ts": 1, "event": "maker_fill", "passphrase": "x"}, {"ts": 2, "event": "settlement"}])
    h = http_api._make_handler(lambda: {}, tmp_path / "t.jsonl", journal_path=p)
    httpd = _serve(h)
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        body = urllib.request.urlopen(base + "/journal?events=maker_fill,settlement&since=0").read().decode()
        rows = [json.loads(l) for l in body.splitlines()]
        assert [r["event"] for r in rows] == ["maker_fill", "settlement"]
        assert "passphrase" not in rows[0]
        try:
            urllib.request.urlopen(base + "/journal?limit=abc")
            assert False
        except urllib.error.HTTPError as e:
            assert e.code == 400
        # unreadable path (directory) -> 500, server still alive
        h2 = http_api._make_handler(lambda: {}, tmp_path / "t.jsonl", journal_path=tmp_path)
        s2 = _serve(h2)
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{s2.server_address[1]}/journal")
            assert False
        except urllib.error.HTTPError as e:
            assert e.code == 500
        s2.shutdown()
        assert urllib.request.urlopen(base + "/journal").status == 200
    finally:
        httpd.shutdown()
