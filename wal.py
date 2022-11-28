import pickle, zlib, os, struct
import shutil

class Wal:
    
    SIZE_UNITS = ['bytes', 'KB', 'MB', 'GB', 'TB']
    WAL_DIR = "_wal" 
    
    def __init__(self) -> None:
        self.snapshot = 1
        self.segment_filename_maximum_size = 20
        self.filename = "{0}/{1}".format(Wal.WAL_DIR,self.get_filename(self.snapshot))
        self.data = {dict}
        self.snap_memtable = {}
        self.offset = 0
        self.create_dir()
    
    def create_dir(self):
        if not os.path.exists(Wal.WAL_DIR):
            os.mkdir(Wal.WAL_DIR)

    def get_crc32(self,data):
        return zlib.crc32(data)

    def convert_bytes(self, num):
        for x in self.SIZE_UNITS:
            if num < 1024.0:
                return "%3.1f %s" % (num, x)
            num /= 1024.0

    def file_size(self, file_path):
        if os.path.isfile(file_path):
            file_info = os.stat(file_path)
            return self.convert_bytes(file_info.st_size)
    
    def get_filename_path(self, name=0):
        return "{0}/{1}".format(Wal.WAL_DIR,self.get_filename(self.snapshot))
    
    def get_filename(self,name=0):
        name = "{0:0{1}d}".format(name,self.segment_filename_maximum_size)
        return name
    
    def construct_payload(self, data, op="PUT"):
        # data = struct.pack("{}s".format(len(data)+1), data.encode('UTF-8'))
        data["OP"] = op
        data["SNAP"] = self.get_filename(self.offset)
        data = {data["SNAP"]: data}
        return data

    def read(self, offset=0):
        f = open(self.filename, "rb")
        f.seek(offset)
        data = pickle.load(f)
        f.close()
        return data
    
    def write(self, data):
        self.offset += 1
        self.filename = self.get_filename_path(self.snapshot)
        f = open(self.filename, "ab")
        data = self.construct_payload(data)
        self.snap_memtable[self.get_filename(self.offset)] = f.tell()
        data = pickle.dump(data,f)
        f.close()
    
    def cleanup_wal(self):
        shutil.rmtree(self.WAL_DIR)

# w = Wal()
# w.write({"data":"demo segment 100"})
# print(" --- ", w.read())
# w.write({"data":"demo segment 200"})
# print(" --- ", w.read())
# print(w.snap_memtable)
# w.write({"data":"demo segment 300"})
# # print(w.read())
# print(w.snap_memtable)