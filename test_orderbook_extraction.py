"""Tests for Polymarket orderbook extraction and complementary book inference."""
import unittest
from polymarket_client import extract_best_bid_ask, extract_market_top
from feeds import PolymarketBookStream


class TestOrderbookExtraction(unittest.TestCase):
    def test_extract_best_bid_ask_with_ascending_bids_descending_asks(self):
        # Polymarket CLOB format: bids ascending (0.01 -> highest), asks descending (0.99 -> lowest)
        book = {
            "bids": [
                {"price": "0.01", "size": "1000"},
                {"price": "0.10", "size": "500"},
                {"price": "0.45", "size": "200"},
                {"price": "0.52", "size": "100"},
            ],
            "asks": [
                {"price": "0.99", "size": "1000"},
                {"price": "0.80", "size": "500"},
                {"price": "0.58", "size": "200"},
                {"price": "0.55", "size": "100"},
            ],
        }
        best_bid, best_ask = extract_best_bid_ask(book)
        self.assertEqual(best_bid, 0.52)
        self.assertEqual(best_ask, 0.55)

    def test_extract_market_top_with_complementary_token(self):
        # In binary markets, UP token might only have bids, while DOWN token has bids
        book_up = {
            "bids": [{"price": "0.58", "size": "500"}],
            "asks": [],
        }
        book_down = {
            "bids": [{"price": "0.39", "size": "300"}],
            "asks": [],
        }
        # Implied ask for UP should be 1.0 - best_bid(DOWN) = 1.0 - 0.39 = 0.61
        bid, ask, spread = extract_market_top(book_up, book_down)
        self.assertEqual(bid, 0.58)
        self.assertEqual(ask, 0.61)
        self.assertEqual(spread, 0.03)

    def test_extract_market_top_empty_books(self):
        bid, ask, spread = extract_market_top(None, None)
        self.assertIsNone(bid)
        self.assertIsNone(ask)
        self.assertIsNone(spread)

    def test_book_stream_complementary_get_top(self):
        stream = PolymarketBookStream()
        # Simulate websocket update: token A has bid 0.40, no ask. Token B has bid 0.58, no ask.
        stream._apply_update("token_A", bid=0.40, ask=None, bid_sz=100)
        stream._apply_update("token_B", bid=0.58, ask=None, bid_sz=200)

        top_a = stream.get_top("token_A", opp_token_id="token_B")
        self.assertIsNotNone(top_a)
        self.assertEqual(top_a["bid"], 0.40)
        # Implied ask = 1.0 - 0.58 = 0.42
        self.assertEqual(top_a["ask"], 0.42)
        self.assertEqual(top_a["spread"], 0.02)


if __name__ == "__main__":
    unittest.main()
