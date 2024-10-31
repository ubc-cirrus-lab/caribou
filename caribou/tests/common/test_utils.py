import unittest
from caribou.common.utils import decompress_json_str, get_function_source
from caribou.common.utils import compress_json_str
import zstandard as zstd


class TestGetFunctionSource(unittest.TestCase):
    def test_get_function_source(self):
        def test_function():
            print("Hello, world!")

        source_code = get_function_source(test_function)

        source_code = "".join(source_code.split())

        self.assertIn('print("Hello,world!")', source_code)

    def test_get_function_source_with_called_function(self):
        def called_function():
            print("Hello from called function!")

        def test_function():
            print("Hello, world!")
            called_function()

        source_code = get_function_source(test_function)

        source_code = "".join(source_code.split())

        self.assertIn('print("Hello,world!")', source_code)

    def test_compress_json_str(self):
        json_str = '{"key": "value"}'
        compressed_bytes = compress_json_str(json_str)

        # Decompress to verify
        dctx = zstd.ZstdDecompressor()
        decompressed_bytes = dctx.decompress(compressed_bytes)
        decompressed_str = decompressed_bytes.decode("utf-8")

        self.assertEqual(json_str, decompressed_str)

    def test_compress_json_str_with_different_compression_level(self):
        json_str = '{"key": "value"}'
        compressed_bytes = compress_json_str(json_str, compression_level=10)

        # Decompress to verify
        dctx = zstd.ZstdDecompressor()
        decompressed_bytes = dctx.decompress(compressed_bytes)
        decompressed_str = decompressed_bytes.decode("utf-8")

        self.assertEqual(json_str, decompressed_str)

    def test_decompress_json_str(self):
        json_str = '{"key": "value"}'
        compressed_bytes = compress_json_str(json_str)
        decompressed_str = decompress_json_str(compressed_bytes)

        self.assertEqual(json_str, decompressed_str)

    def test_decompress_json_str_with_different_compression_level(self):
        json_str = '{"key": "value"}'
        compressed_bytes = compress_json_str(json_str, compression_level=10)
        decompressed_str = decompress_json_str(compressed_bytes)

        self.assertEqual(json_str, decompressed_str)

    def test_decompress_json_str_with_invalid_data(self):
        invalid_data = b"invalid compressed data"
        with self.assertRaises(zstd.ZstdError):
            decompress_json_str(invalid_data)


if __name__ == "__main__":
    unittest.main()
