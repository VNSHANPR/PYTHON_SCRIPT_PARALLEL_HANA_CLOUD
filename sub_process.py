#!/usr/bin/env python3
import sys
from hdbcli import dbapi
import hana_ml.dataframe as dataframe
from datetime import datetime
import pandas as pd
import json
import requests


from numpy import load
from numpy import save
from datetime import datetime
from datetime import timedelta

print(sys.argv[1])
def HANA_CUSTOM_SQL(UserName):
    
    host_name='e3bd540b-325a-4e89-8404-1e17a1a6cbb8.hna0.prod-eu10.hanacloud.ondemand.com'
    passwd='Welcome01'
    sql_cmd_list='CREATE COLUMN TABLE TAXI LIKE "HC_DEMO"."TAXI" WITH DATA;ALTER TABLE TAXI ADD (SHAPE ST_GEOMETRY(5018));UPDATE TAXI SET SHAPE = ST_GEOMFROMTEXT(POLYLINE, 4326).ST_TRANSFORM(5018);ALTER TABLE TAXI ADD (STARTPOINT ST_GEOMETRY(5018), ENDPOINT ST_GEOMETRY(5018));UPDATE TAXI SET STARTPOINT = SHAPE.ST_STARTPOINT(), ENDPOINT = SHAPE.ST_ENDPOINT();ALTER TABLE TAXI ADD (DURATION INTEGER);UPDATE TAXI SET DURATION = (SHAPE.ST_NUMPOINTS() - 1) * 15;ALTER TABLE TAXI ADD (STARTTIME TIMESTAMP, ENDTIME TIMESTAMP);UPDATE TAXI SET STARTTIME = TIMESTAMP, ENDTIME = ADD_SECONDS(TIMESTAMP, DURATION);ALTER TABLE TAXI ADD (DISTANCE INTEGER);UPDATE TAXI SET DISTANCE = TO_INTEGER(SHAPE.ST_LENGTH());ALTER TABLE TAXI ADD (SPEED_AVG INTEGER);UPDATE TAXI SET SPEED_AVG = TO_INTEGER(DISTANCE/DURATION * 3.6);SELECT ST_CONVEXHULLAGGR(SHAPE).ST_TRANSFORM(4326).ST_ASWKT() FROM TAXI;CREATE COLUMN TABLE OSM_POI LIKE "HC_DEMO".osm_poi WITH DATA;CREATE COLUMN TABLE REFGRID LIKE "HC_DEMO".REFGRID WITH DATA;SELECT HEXID, HEXCENTROID.ST_TRANSFORM(4326).ST_ASWKT() AS HEXCENTROID,HEXCELL.ST_TRANSFORM(4326).ST_ASWKT() AS HEXCELL FROM REFGRID;SELECT TOP 1000 INDEX, TRIP_ID, CALL_TYPE, TAXI_ID, STARTTIME, ENDTIME,SPEED_AVG,SHAPE.ST_TRANSFORM(4326).ST_ASWKT() as SHAPE FROM TAXI ORDER BY RAND();SELECT B.OSMID, B.SHAPE.ST_TRANSFORM(4326).ST_ASWKT() AS OSMSHAPE, B.AMENITY, B.NAME, A.HEXCELL.ST_TRANSFORM(4326).ST_ASWKT() AS HEXSHAPE FROM (SELECT TOP 3 ST_CLUSTERCELL() AS HEXCELL FROM TAXI GROUP CLUSTER BY STARTPOINT USING HEXAGON X CELLS 500 ORDER BY COUNT(*) DESC) A LEFT JOIN OSM_POI B ON A.HEXCELL.ST_INTERSECTS(B.SHAPE) = 1;'
    sql_cmd_list=sql_cmd_list.rstrip()
    sql_cmd_list=sql_cmd_list.strip("\n")
    sql_cmd=[]
    sql_cmd=sql_cmd_list.split(";")
    df_testing=pd.DataFrame(columns=["User_Name","Test Name","Result","TimeStamp","Exception"])
    #query_container="SELECT * FROM SYSHDL.CONTAINERS where CONTAINER='SYSHDL_HDLTA_CONTAINER'"
    #query_remote_source="CALL CHECK_REMOTE_SOURCE('SYSHDL_HDLTA_CONTAINER_SOURCE')"
    
    try:
        conn_hana_ta = dbapi.connect(address = host_name,
                                    port = 443, 
                                    user = UserName, 
                                    password = passwd, 
                                    encrypt = 'true',
                                    sslValidateCertificate = 'false' 
                                    )
                                    
    
        cursor = conn_hana_ta.cursor()
        data={"User_Name":UserName,"Test Name":"Login to the Database as User "+UserName,"Result":"Pass","TimeStamp":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"Exception":"Login Successful"}
        df_testing=df_testing.append(data,ignore_index=True)
        

    except :
        data={"User_Name":UserName,"Test Name":"Could not Execute SQL List for the User "+UserName,"Result":"Failed","TimeStamp":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"Exception":sql_cmd}
        df_testing=df_testing.append(data,ignore_index=True)

    try:
        for i in range(len(sql_cmd)-1):
            try:
                print("Executing Steps for user :"+UserName )
                print("trying to execute SQL number: "+str(i))
                print(sql_cmd[i])
                cursor.execute(sql_cmd[i])
                print("Done")               
                data={"User_Name":UserName,"Test Name":sql_cmd[i],"Result":"Pass","TimeStamp":datetime.now().strftime("%Y-%m-%d %H:%M"),"Exception":"None"}
                df_testing=df_testing.append(data,ignore_index=True)
                
            except Exception as inst1:
                print(inst1)
                data={"User_Name":UserName,"Test Name":sql_cmd[i],"Result":"Failed","TimeStamp":datetime.now().strftime("%Y-%m-%d %H:%M"),"Exception":"Failed to execute SQL"}
                df_testing=df_testing.append(data,ignore_index=True)
            
        conn_hana_ta.close()

    except:
        data={"User_Name":UserName,"Test Name":"Could not Execute SQL List for the User "+UserName,"Result":"Failed","TimeStamp":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"Exception":"Could not Iterate over the SQL list provided"}
        df_testing=df_testing.append(data,ignore_index=True)
    filename=UserName+".csv"
    df_testing.to_csv(filename)
    return df_testing

HANA_CUSTOM_SQL(sys.argv[1])
