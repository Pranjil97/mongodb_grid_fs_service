from pymongo import MongoClient
import gridfs

class Mongogrid():

    def __init__(self,connection_string) -> bool:

        self.connection_string = connection_string
        self.connection = MongoClient(self.connection_string)
        self.grid_fs = self.connection["grid_fs"]
        self.fs=gridfs.GridFS(self.grid_fs)

    def upload_file(self,filename:str, data:bytes) -> bool:
        try:
            self.fs.put(data=data, filename=filename)
            return True
        except Exception as e:
            print(f"grid_fs_operations :: create_file :: {filename} :: {e}")
            return False

    def delete(self,filename:str)-> bool:
        try:
            data=[i for i in self.grid_fs.fs.files.find({"filename":filename})]
            data_id=data[0]["_id"]
            self.fs.delete(data_id)
            return True
        except Exception as e:
            print(f"grid_fs_operations :: delete_file :: {filename} :: {e}")
            return False

    def update(self,filename:str, data:bytes) -> bool:
        try:
            if self.fs.exists({"filename":filename}):
                d=[i for i in self.grid_fs.fs.files.find({"filename": filename})]
                data_id=d[0]["_id"]
                self.fs.delete(data_id)
            print(type(data))
            print(data)
            self.fs.put(data=data, filename=filename)
            return True
        except Exception as e:
            print(f"grid_fs_operations :: update_file :: {filename} :: {e}")
            return False 

    def read(self,filename:str):
        try:
            data=self.grid_fs.fs.files.find_one({"filename":filename})
            id=data["_id"]
            data_content=self.fs.get(id)
            content=data_content.read()
            return content
        except Exception as e:
            print(f"grid_fs_operations :: read :: {e}")
            return None

    def list_file(self):
        try:
            data=([i for i in self.grid_fs.fs.files.find({},{"_id":False})])
            return data
        except Exception as e:
            print(f"grid_fs_operations :: list :: {e}")