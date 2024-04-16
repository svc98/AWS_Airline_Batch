import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node daily_flights_data
daily_flights_data_node1713198466977 = glueContext.create_dynamic_frame.from_catalog(
    database="airline_db",
    table_name="raw_daily_flights",
    transformation_ctx="daily_flights_data_node1713198466977")

# Script generated for node airport_dim
airport_dim_node1713198505121 = glueContext.create_dynamic_frame.from_catalog(
    database="airline_db",
    table_name="tbl_dev_airlines_airports_dim",
    redshift_tmp_dir="s3://redshift-temp-airline/temp-data/airline-dim/",
    transformation_ctx="airport_dim_node1713198505121")

# Script generated for node Filter
Filter_node1713198490389 = Filter.apply(frame=daily_flights_data_node1713198466977, f=lambda row: (row["depdelay"] >= 60), transformation_ctx="Filter_node1713198490389")

# Script generated for node Join - Dept Airport
Filter_node1713198490389DF = Filter_node1713198490389.toDF()
airport_dim_node1713198505121DF = airport_dim_node1713198505121.toDF()
JoinDeptAirport_node1713198540786 = DynamicFrame.fromDF(Filter_node1713198490389DF.join(airport_dim_node1713198505121DF, (Filter_node1713198490389DF['originairportid'] == airport_dim_node1713198505121DF['airport_id']), "left"), glueContext, "JoinDeptAirport_node1713198540786")

# Script generated for node modify_dept_airport_cols
modify_dept_airport_cols_node1713198607187 = ApplyMapping.apply(
    frame=JoinDeptAirport_node1713198540786,
    mappings=[
        ("destairportid", "long", "destairportid", "long"),
        ("carrier", "string", "carrier", "string"),
        ("depdelay", "long", "dep_delay", "bigint"),
        ("arrdelay", "long", "arr_delay", "bigint"),
        ("city", "string", "dep_city", "string"),
        ("name", "string", "dep_airport", "string"),
        ("state", "string", "dep_state", "string")],
    transformation_ctx="modify_dept_airport_cols_node1713198607187")

# Script generated for node Join - Arr Airport
modify_dept_airport_cols_node1713198607187DF = modify_dept_airport_cols_node1713198607187.toDF()
airport_dim_node1713198505121DF = airport_dim_node1713198505121.toDF()
JoinArrAirport_node1713206492846 = DynamicFrame.fromDF(modify_dept_airport_cols_node1713198607187DF.join(airport_dim_node1713198505121DF, (modify_dept_airport_cols_node1713198607187DF['destairportid'] == airport_dim_node1713198505121DF['airport_id']), "left"), glueContext, "JoinArrAirport_node1713206492846")

# Script generated for node modify_arr_airport_cols
modify_arr_airport_cols_node1713206602000 = ApplyMapping.apply(
    frame=JoinArrAirport_node1713206492846,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("dep_delay", "bigint", "dep_delay", "long"),
        ("arr_delay", "bigint", "arr_delay", "long"),
        ("date", "string", "date", "string"),
        ("dep_city", "string", "dep_city", "string"),
        ("dep_airport", "string", "dep_airport", "string"),
        ("dep_state", "string", "dep_state", "string"),
        ("city", "string", "arr_city", "string"),
        ("name", "string", "arr_airport", "string"),
        ("state", "string", "arr_state", "string")],
    transformation_ctx="modify_arr_airport_cols_node1713206602000")

# Script generated for node Redshift_Target_Table
Redshift_Target_Table_node1713210857157 = glueContext.write_dynamic_frame.from_catalog(frame=modify_arr_airport_cols_node1713206602000, database="airline_db", table_name="tbl_dev_airlines_daily_flights_fact", redshift_tmp_dir="s3://redshift-temp-airline/temp-data/airline-fact/",additional_options={"aws_iam_role": "arn:aws:iam::891377180984:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T160936"}, transformation_ctx="Redshift_Target_Table_node1713210857157")

job.commit()