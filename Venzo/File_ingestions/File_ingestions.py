import os
import sys
import pandas as pd

#DataBase Module
from sqlalchemy import create_engine

    
    #Class for Daily OFAC Report 
class File_Ingestion:

        #init function for the Path, Table_Name and Database        
    def __init__(self, Path, Table_Name, Database_name):
        
        try:
                #Path and Table_name Initialization
            self.Path = Path
            self.Table_Name = Table_Name
            self.Database_name = Database_name
            self.User_Name = ''
            self.Password = ''
            self.Hostname = ''
                
                #Database Connectivity            
            self.sqlEngine_decision = create_engine('mysql+pymysql://' + str(Hostname) + ':' + 
                                                    str(Hostname) + '@' + 
                                                    str(Hostname) + ':3306/' + 
                                                    str(Database_name))
            self.dbConnection_decision = self.sqlEngine_decision.connect()
            
        except Exception as exe:
            print(exe)
            
    
    def Read__Upload_Action(self):
        
        try:
                #Read the CSV File
            Read_csv =  pd.read_csv(self.Path, sep=',', index_col=False, dtype='unicode')
                
                #Uploading the Data into Table
            data.to_sql(con=self.dbConnection_decision, name=self.Table_Name, if_exists='replace', index = False) 
            
        except Exception as exe:
            print(exe)    
                

if __name__ == '__main__':
    
    args = sys.argv
    
    if len(args) == 4: 
        Path = args[1]
        Database_name = args[2]
        Table_Name = args[3]
        
        File_size = os.path.getsize(Path)
        if File_size > 1073741824:
            FI =  File_Ingestion(Path, Table_Name)
            FI.Read__Upload_Action()
        else:
            print("File size is less than 1 GB. Therefore, action cannot be perform. So, Cross check the filesize.")
    else:
        print("File_Path and Table_Name is Required")   
