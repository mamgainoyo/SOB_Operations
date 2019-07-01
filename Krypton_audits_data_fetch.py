import pandas as pd
import prestodb
from datetime import datetime,timedelta
import os
from math import ceil


############### Inputs #################

config_id=''
trigger_date=''
end_date=''

# Connection String

conn=prestodb.dbapi.connect(
    host='',
    port=,
    user='',
    catalog='hive',
    schema='',
    http_scheme='http',
    
)
########################################

#Function to run the query on prestodb
def run_me(sql):
    #result = pd.read_sql_query(sql,conn_hive)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    colnames = [part[0] for part in cur.description]
    cur.cancel()
    #conn.close()

    return(pd.DataFrame(rows, columns=colnames))

day= int(trigger_date[8:])
start_date= pd.to_datetime(trigger_date[:10])
end_date= pd.to_datetime(end_date[:10])

# Query
sql1="""
SELECT oyo_id,ques,ans
FROM 
( 
  SELECT hotels_summary.oyo_id as oyo_id, from_unixtime(cast(substr(audittask_base.createdon,13,13) as bigint)/1000) as trigger_date, 
  from_unixtime(cast(substr(audittask_base.taskcompletiondatetime,13,13) as bigint)/1000) as completion_date,
  cast(TRANSFORM(CAST(JSON_PARSE(audittask_base.qlist) AS ARRAY<JSON>), x -> JSON_EXTRACT_SCALAR(x, '$.questionId')) as array<varchar>) as questions,
  cast(TRANSFORM(CAST(JSON_PARSE(audittask_base.qlist) AS ARRAY<JSON>), x -> JSON_EXTRACT_SCALAR(x, '$.answer')) as array<varchar>) as answers
  from task_service.audittask_base 
  inner join aggregatedb.hotels_summary on task_service.audittask_base.entityid=cast(aggregatedb.hotels_summary.hotel_id as varchar)
  where taskconfigid= '{0}' and
  from_unixtime(cast(substr(audittask_base.createdon,13,13) as bigint)/1000) between date('{1}') and date('{2}')
  and audittask_base.qlist!='[e,m,p,t,y]'
)
CROSS JOIN UNNEST(questions, answers) AS t (ques, ans)
"""

# Making date partitions to pull from hive
# Makes the map-reduce faster
# Also the code fails if (Rate exceeded if you pull for a lot of dates together)
def getDate(i):
    
    if(i==0):
        date1 = start_date
    else:
        date1 = start_date + timedelta(5*i)
    
    
    date2 = date1 + timedelta(days = 4)
    if(end_date<date2):
        date2=end_date
    #date_i1 = date1 + timedelta(days = -1)
    #date_i2 = date2 + timedelta(days = +1)
    
    #inserted_at = {'inserted_at':[date1.strftime("%Y%m%d"),date2.strftime("%Y%m%d")]}
    created_at = {'created_at':[date1.strftime("%Y-%m-%d"),date2.strftime("%Y-%m-%d")]}
    return(created_at)

number_of_weeks= int(ceil((int(day)+2)/5))

dateSplit = [ getDate(i) for i in range (0,number_of_weeks,1)]

# Running week wise now

for i in dateSplit:
    print(sql1.format(config_id,i['created_at'][0],i['created_at'][1]))
    temp=run_me(sql1.format(config_id,i['created_at'][0],i['created_at'][1]))
    
    try:
        mtd=pd.concat([mtd,temp])
    except:
        mtd=temp


mtd.to_csv('data_dump.csv')

