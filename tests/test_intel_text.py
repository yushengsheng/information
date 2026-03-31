from __future__ import annotations

import unittest
from unittest.mock import patch

from services.intel import text


class IntelTextTests(unittest.TestCase):
    def test_should_translate_to_chinese_keeps_chinese_and_accepts_foreign_languages(self) -> None:
        self.assertFalse(text.should_translate_to_chinese("这是中文原文"))
        self.assertTrue(text.should_translate_to_chinese("Bitcoin breaks above 100k"))
        self.assertTrue(text.should_translate_to_chinese("ロシアが新たな制裁対応を発表"))
        self.assertTrue(text.should_translate_to_chinese("Россия объявила новые меры"))
        self.assertFalse(text.should_translate_to_chinese("https://example.com/test"))

    @patch.object(text, "translate_text_to_chinese", autospec=True)
    def test_populate_display_text_translates_foreign_text_and_keeps_chinese_original(
        self,
        mock_translate_text_to_chinese,
    ) -> None:
        mock_translate_text_to_chinese.side_effect = lambda value: {
            "Bitcoin breaks above 100k": "比特币突破 10 万美元",
            "Россия объявила новые меры": "俄罗斯宣布新的应对措施",
        }.get(value, value)
        items = [
            {"text": "Bitcoin breaks above 100k"},
            {"text": "这是中文原文"},
            {"text": "Россия объявила новые меры"},
        ]

        populated = text.populate_display_text(items)

        self.assertEqual(populated[0]["original_text"], "Bitcoin breaks above 100k")
        self.assertEqual(populated[0]["display_text"], "比特币突破 10 万美元")
        self.assertEqual(populated[1]["original_text"], "这是中文原文")
        self.assertEqual(populated[1]["display_text"], "这是中文原文")
        self.assertEqual(populated[2]["display_text"], "俄罗斯宣布新的应对措施")


if __name__ == "__main__":
    unittest.main()
