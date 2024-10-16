from abc import ABC, abstractmethod

class IDBManager(ABC):

    @abstractmethod
    def open_connection(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def insert_data(self, data):
        pass

    @abstractmethod
    def close_connection(self):
        pass