import json

class JsonReader:
    def __init__(self, file_path):
        self.__file_path = file_path

    def read(self) -> dict:
        with open(self.__file_path, 'r') as file:
            return json.load(file)