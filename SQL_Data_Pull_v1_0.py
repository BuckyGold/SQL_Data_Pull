# This is to extract the data from a SQL Servers
# -*- coding: utf-8 -*-
"""
Created on 12-1-2021

@author BuckyGold

"""
import pandas as pd
import pyodbc
import time
from datetime import datetime

start_time = time.time()

print('Connecting to Database...')

SourcePath = 'C:\\Users\\username\MainFolder\GroupFolder\Source/'
LogPath = 'C:\\Users\\username\MainFolder\GroupFolder\Log/'

LogFile = open(LogPath+'SQL_Load_Log_'+datetime.now().strftime('%Y%m%d_%H%M%S')+'.csv','w+')

LogFile.write('DateTime,Status,ReadTime,WriteTime'+'\n')

server = 'sql.database.windows.net'
database = 'sql-db'
username = 'UserName1'
password = 'Uniqu3P@ssw0rdThatT0t@11yM@k3s53ns3'
driver = '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect('driver='+driver+';server='+server+';PORT=1433;database='+database+';UID='+username+';PWD='+ password)

TaskList = ['Task1', 'Task2', 'Task3']

for TaskName in  TaskList:
    print('Information Gathering...Started')
    print(TaskName)
    StatusGood = True

    try:
        file_read_time_start = time.time()
        TempTable =  pd.read_sql_query("""SELECT
        TABLE.Column1
        ,TABLE.Column2
        ,TABLE2.Column1
        ,TABLE2.Column2
        FROM [sql-db].[data].[TABLE] TABLE
        LEFT JOIN [sql-db].[data].[TABLE2] TABLE2
        ON TABLE.Column1 = TABLE2.Column1
        WHERE [TaskName]='"""+TaskName+"""'
        UNION
        SELECT
        TABLE3.Column1
        ,TABLE3.Column2
        ,TABLE4.Column1
        ,TABLE4.Column2
        ,TABLE4.Column3
        FROM [sql-db].[data].[TABLE3] TABLE3
        LEFT JOIN [sql-db].[data].[TABLE4] TABLE4
        ON TABLE3.Column1 = TABLE4.Column1
        WHERE [TaskName]='"""+TaskName+"""'
        ;
        """, conn)
        file_read_time = time.time() - file_read_time_start
    except Exception as e:
        StatusGood = False
        print(e)
        LogFile.write(datetime.now().strftime('%d/%m/%Y %H:%M:%S')+ ',' + TaskName +', Failed '+ str(e) + '\n')
    
    try:
        file_write_time_start = time.time()
        TempTable.to_excel(SourcePath+TaskName+'.xlsx', encoding='utf-8', index=False)
        file_write_time = time.time() - file_write_time_start
    except Exception as e:
        StatusGood = False
        print(e)
        LogFile.write(datetime.now().strftime('%d/%m/%Y %H:%M:%S')+','+ TaskName +', Failed: '+ str(e)+'\n')

    if StatusGood:
        print(datetime.now().strftime('%d/%m/%Y %H:%M:%S')+' Procedure: '+TaskName+' Rows: '+str(len(TempTable.index))+' Read Time: '+ str(round(file_read_time, 2)) + ' Sec Write Time: ' + str(round(file_write_time,2)) + ' sec')
        print('Information Gathering...Complete')
        print('Saving Results')
        LogFile.write(datetime.now().strftime('%d/%m/%Y %H:%M:%S')+','+TaskName+' ,PASS,'+ str(len(TempTable.index))+','+ str(round(file_read_time,2))+',' + str(round(file_write_time,2))+'\n')
        TempTable = pd.DataFrame()

end_time = str(round((time.time()-start_time),2))
time_stamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

LogFile.write(time_stamp+', Total Time, 0, '+ end_time +'\n')
LogFile.close()
print("--- %s seconds ---"%(time.time() - start_time))
