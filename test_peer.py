import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from peer import Peer, PeerError, _rpc, create_metadata, load_metadata, save_metadata


def free_port() -> int:
    import socket

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class PeerTransferTests(unittest.TestCase):
    def test_metadata_has_per_block_and_file_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source.bin"
            source.write_bytes(bytes(range(256)) * 9)
            metadata = create_metadata(source, block_size=1024)

            self.assertEqual(metadata["file_size"], 2304)
            self.assertEqual(metadata["block_count"], 3)
            self.assertEqual(metadata["sha256"], hashlib.sha256(source.read_bytes()).hexdigest())
            self.assertEqual(len(metadata["block_hashes"]), 3)

    def test_two_peers_transfer_and_verify_file(self) -> None:
        content = bytes(range(256)) * 20 + b"fim"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "original.bin"
            metadata_file = root / "metadata.json"
            output = root / "downloads" / "copy.bin"
            source.write_bytes(content)
            metadata = create_metadata(source, block_size=333)
            save_metadata(metadata_file, metadata)
            loaded_metadata = load_metadata(metadata_file)

            seed = Peer(
                "127.0.0.1",
                free_port(),
                loaded_metadata,
                root / "seed",
                seed_file=source,
                peer_id="seed-test",
            )
            leecher = Peer(
                "127.0.0.1",
                free_port(),
                loaded_metadata,
                root / "leecher",
                neighbors=[("127.0.0.1", seed.port)],
                peer_id="leecher-test",
            )
            seed.start()
            leecher.start()
            try:
                leecher.download(output, timeout=10, workers=3)
            finally:
                leecher.stop()
                seed.stop()

            self.assertEqual(output.read_bytes(), content)
            self.assertEqual(hashlib.sha256(output.read_bytes()).hexdigest(), metadata["sha256"])
            self.assertTrue(all(leecher.have()))

    def test_metadata_query_and_leecher_shares_with_downstream_peer(self) -> None:
        content = bytes(range(251)) * 42
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "original.bin"
            metadata_file = root / "metadata.json"
            source.write_bytes(content)
            metadata = create_metadata(source, block_size=4096)
            save_metadata(metadata_file, metadata)
            loaded_metadata = load_metadata(metadata_file)

            seed = Peer(
                "127.0.0.1",
                free_port(),
                loaded_metadata,
                root / "seed",
                seed_file=source,
                peer_id="seed-chain-test",
            )
            first_leecher = Peer(
                "127.0.0.1",
                free_port(),
                loaded_metadata,
                root / "first",
                neighbors=[("127.0.0.1", seed.port)],
                peer_id="first-chain-test",
            )
            downstream = Peer(
                "127.0.0.1",
                free_port(),
                loaded_metadata,
                root / "downstream",
                neighbors=[("127.0.0.1", first_leecher.port)],
                peer_id="downstream-chain-test",
            )
            seed.start()
            first_leecher.start()
            downstream.start()
            try:
                response, _ = _rpc(("127.0.0.1", seed.port), {"op": "metadata"})
                self.assertEqual(response["metadata"], loaded_metadata)
                first_leecher.download(root / "first.bin", timeout=10, workers=3)
                downstream.download(root / "downstream.bin", timeout=10, workers=3)
            finally:
                downstream.stop()
                first_leecher.stop()
                seed.stop()

            self.assertEqual((root / "first.bin").read_bytes(), content)
            self.assertEqual((root / "downstream.bin").read_bytes(), content)
            self.assertTrue(all(downstream.have()))

    def test_rejects_inconsistent_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            metadata_file = Path(temporary) / "bad.json"
            metadata_file.write_text(
                json.dumps(
                    {
                        "file_id": "0" * 64,
                        "filename": "bad.bin",
                        "file_size": 1,
                        "block_size": 1,
                        "block_count": 2,
                        "sha256": "0" * 64,
                        "block_hashes": ["0" * 64, "0" * 64],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(PeerError):
                load_metadata(metadata_file)


if __name__ == "__main__":
    unittest.main()
