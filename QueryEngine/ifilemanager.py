from abc import ABC, abstractmethod

class IFileManager(ABC):

    def __init__(self, readfrom_folderpath = None, writeto_folderpath = None):
        self.readfrom_folderpath = readfrom_folderpath
        self.writeto_folderpath = writeto_folderpath

    @abstractmethod
    def read(self, filename):
        '''
        read filename from self.readfrom_folderpath
        '''
    @abstractmethod
    def write(self, filename, data):
        '''
        write data to self.writeto_folderpath + filename 
        '''