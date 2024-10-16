import numpy as np 
import pandas as pd

def prepare_data():
    
    cnt = 1

    df = pd.read_csv("C:\\Users\\dk957\\Downloads\\archive (2)\\train.csv")

    columns = [column for column in df.columns]

    # for column in df.columns:
    #     if cnt < 562:
    #         string = f"{column.replace(',','-')}"
    #         columns.append(string)
    #         cnt += 1
    #     elif cnt == 562:
    #         string = f"{column.replace(',','-')}"
    #         columns.append(string)
    #         cnt += 1
    #     else:
    #         string = f"{column.replace(',','-')} text"
    #         columns.append(string)
    #         cnt += 1
        
    return columns
            
if __name__ == '__main__':
    prepare_data()

