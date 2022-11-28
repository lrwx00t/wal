import unittest, os
from wal import Wal

class WalTest(unittest.TestCase):
    def test_wal_dir_created_or_exists(self):
        Wal()
        self.assertTrue(os.path.exists(Wal.WAL_DIR))

    def test_wal_segment_created_or_exists(self):
        first_segment_file = "00000000000000000001"
        expected_segment_path = "{0}/{1}".format(Wal.WAL_DIR,first_segment_file)
        Wal().write({"data":"demo segment"})
        self.assertTrue(os.path.exists(expected_segment_path))
    
    def test_wal_verify_segment_contents(self):
        first_segment_file = "00000000000000000001"
        expected_snapshot_memetable = {'00000000000000000001': 0}
        test_data = {"data":"demo segment"}
        expected_result = {'00000000000000000001': {'OP': 'PUT','SNAP': '00000000000000000001','data': 'demo segment'}}
        expected_segment_path = "{0}/{1}".format(Wal.WAL_DIR,first_segment_file)
        w = Wal()
        w.write(test_data)
        self.assertTrue(os.path.exists(expected_segment_path))
        self.assertDictEqual(w.read(), expected_result)
        self.assertDictEqual(w.snap_memtable, expected_snapshot_memetable)
    
    def test_wal_verify_adding_two_segment(self):
        first_segment_file = "00000000000000000001"
        expected_snapshot_memetable = {'00000000000000000001': 0}
        test_data = {"data":"demo segment"}
        expected_result = {'00000000000000000001': {'OP': 'PUT','SNAP': '00000000000000000001','data': 'demo segment'}}
        expected_segment_path = "{0}/{1}".format(Wal.WAL_DIR,first_segment_file)
        w = Wal()
        w.write(test_data)
        test_data = {"data":"demo segment"}
        w.write(test_data)
        # self.assertTrue(os.path.exists(expected_segment_path))
        # self.assertDictEqual(w.read(), expected_result)
        # self.assertDictEqual(w.snap_memtable, expected_snapshot_memetable)

    def test_cleanup_wal(self):
        Wal().cleanup_wal()
        self.assertFalse(os.path.exists(Wal.WAL_DIR))
    
    def tearDown(self) -> None:
        Wal().cleanup_wal()
    
    @classmethod
    def tearDownClass(cls):
        Wal().cleanup_wal()
    

if __name__ == '__main__':
    unittest.main()