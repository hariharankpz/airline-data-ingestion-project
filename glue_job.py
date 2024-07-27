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

# Script generated for node airline-redshift-dim
airlineredshiftdim_node1722071170579 = glueContext.create_dynamic_frame.from_catalog(database="airline-database", table_name="dev_airlines_airports_dim",redshift_tmp_dir="s3://airline-data-temp-dir/dim-table-temp/", transformation_ctx="airlineredshiftdim_node1722071170579")

# Script generated for node s3-raw-data-catalog
s3rawdatacatalog_node1722070344186 = glueContext.create_dynamic_frame.from_catalog(database="airline-database", table_name="daily_flights", transformation_ctx="s3rawdatacatalog_node1722070344186")

# Script generated for node filter-long-dep-delays
filterlongdepdelays_node1722070377476 = Filter.apply(frame=s3rawdatacatalog_node1722070344186, f=lambda row: (row["depdelay"] > 60), transformation_ctx="filterlongdepdelays_node1722070377476")

# Script generated for node join-orginid-dimRedshiftTable
filterlongdepdelays_node1722070377476DF = filterlongdepdelays_node1722070377476.toDF()
airlineredshiftdim_node1722071170579DF = airlineredshiftdim_node1722071170579.toDF()
joinorginiddimRedshiftTable_node1722071297841 = DynamicFrame.fromDF(filterlongdepdelays_node1722070377476DF.join(airlineredshiftdim_node1722071170579DF, (filterlongdepdelays_node1722070377476DF['originairportid'] == airlineredshiftdim_node1722071170579DF['airport_id']), "left"), glueContext, "joinorginiddimRedshiftTable_node1722071297841")

# Script generated for node modify_dept_airport_columns
modify_dept_airport_columns_node1722071696566 = ApplyMapping.apply(frame=joinorginiddimRedshiftTable_node1722071297841, mappings=[("depdelay", "long", "dep_delay", "bigint"), ("arrdelay", "long", "arr_delay", "bigint"), ("destairportid", "long", "destairportid", "long"), ("carrier", "string", "carrier", "string"), ("city", "string", "dep_city", "string"), ("name", "string", "dep_airport", "string"), ("state", "string", "dep_state", "string")], transformation_ctx="modify_dept_airport_columns_node1722071696566")

# Script generated for node  join-destId-dimRedshiftTable
modify_dept_airport_columns_node1722071696566DF = modify_dept_airport_columns_node1722071696566.toDF()
airlineredshiftdim_node1722071170579DF = airlineredshiftdim_node1722071170579.toDF()
joindestIddimRedshiftTable_node1722072700834 = DynamicFrame.fromDF(modify_dept_airport_columns_node1722071696566DF.join(airlineredshiftdim_node1722071170579DF, (modify_dept_airport_columns_node1722071696566DF['destairportid'] == airlineredshiftdim_node1722071170579DF['airport_id']), "left"), glueContext, "joindestIddimRedshiftTable_node1722072700834")

# Script generated for node Modify-arrival-airport-columns
Modifyarrivalairportcolumns_node1722073431737 = ApplyMapping.apply(frame=joindestIddimRedshiftTable_node1722072700834, mappings=[("carrier", "string", "carrier", "string"), ("dep_state", "string", "dep_state", "string"), ("arr_delay", "bigint", "arr_delay", "long"), ("dep_city", "string", "dep_city", "string"), ("dep_delay", "bigint", "dep_delay", "long"), ("dep_airport", "string", "dep_airport", "string")], transformation_ctx="Modifyarrivalairportcolumns_node1722073431737")

# Script generated for node add_target_fact_table
add_target_fact_table_node1722073929391 = glueContext.write_dynamic_frame.from_catalog(frame=Modifyarrivalairportcolumns_node1722073431737, database="airline-database", table_name="dev_airlines_daily_flights_fact", redshift_tmp_dir="s3://airline-data-temp-dir/fact-table-temp/",additional_options={"aws_iam_role": "arn:aws:iam::381491939671:role/redshift-role"}, transformation_ctx="add_target_fact_table_node1722073929391")

job.commit()