import tempfile
import unittest
from pathlib import Path

from scripts.preserve_trx_crash_abandon_cleanup import (
    classify_preserve_artifact,
    cleanup_preserve_artifacts,
)


class PreserveTrxCrashAbandonCleanupTest(unittest.TestCase):
    def test_classifies_only_preserve_artifacts(self):
        self.assertEqual("snapshot", classify_preserve_artifact("tok_1.bin"))
        self.assertEqual("snapshot_tmp",
                         classify_preserve_artifact("tok_1.bin.tmp"))
        self.assertEqual("binlog_cache",
                         classify_preserve_artifact("tok_1.binlog_cache"))
        self.assertEqual("generic_blob",
                         classify_preserve_artifact("tok_1.blob.record_locks"))
        self.assertEqual("generic_blob_tmp",
                         classify_preserve_artifact(
                             "tok_1.blob.record_locks.tmp"))
        self.assertEqual("temp_sidecar",
                         classify_preserve_artifact("tok_1.tempts.42.image"))
        self.assertEqual("warm_artifact",
                         classify_preserve_artifact(
                             "warm_1.record_locks.warm.7.desc"))
        self.assertIsNone(classify_preserve_artifact("ibdata1"))
        self.assertIsNone(classify_preserve_artifact("tok_1.frm"))
        self.assertIsNone(classify_preserve_artifact("../tok_1.bin"))

    def test_dry_run_reports_without_deleting(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "tok_1.bin").write_text("snapshot")
            (root / "tok_1.binlog_cache").write_text("cache")
            (root / "tok_1.tempts.42.image").write_text("image")
            (root / "ibdata1").write_text("native")

            result = cleanup_preserve_artifacts(root, execute=False)

            self.assertEqual(3, len(result.removed))
            self.assertEqual([], result.errors)
            self.assertTrue((root / "tok_1.bin").exists())
            self.assertTrue((root / "ibdata1").exists())

    def test_execute_removes_preserve_artifacts_but_keeps_native_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shard = root / "blob_shards" / "t"
            shard.mkdir(parents=True)
            (root / "tok_1.bin").write_text("snapshot")
            (root / "tok_1.tainted").write_text("tainted")
            (shard / "tok_1.blob.record_locks").write_text("locks")
            (root / "warm_1.record_locks.warm.7").write_text("warm")
            (root / "ibdata1").write_text("native")

            result = cleanup_preserve_artifacts(root, execute=True)

            self.assertEqual([], result.errors)
            self.assertFalse((root / "tok_1.bin").exists())
            self.assertFalse((root / "tok_1.tainted").exists())
            self.assertFalse((shard / "tok_1.blob.record_locks").exists())
            self.assertFalse((root / "warm_1.record_locks.warm.7").exists())
            self.assertTrue((root / "ibdata1").exists())


if __name__ == "__main__":
    unittest.main()
