Dimension table - Contains preloaded airport code - 

Fact table - some measurable attributes will be present, where we will be upserting the data

Glue crawler would need to wait for sometime to be successful and then only the glue job needs to be triggered.Also we would be enabling the job bookmarking.(Hive like partition here in s3 source, so automatically incremental load will be taken care of here by job bookmarking). 

If glue job got triggered even before the crawler completes, glue will be loading empty data, because even though the data is there in s3 in new partition(hive like), until and unless crawler completes its execution, glue catalog is not updated and glue doesn’t know about newly landed data in source and it will consider all old partitions as old data and will not find today's newly landed data and load empty data into it through glue catalog table.

The fact table would be a denormalized table, where it will be having full details of the airport ,including  name, city, state of it, so that we are not joining our raw data again and again with dim table for the details






This is a DIM table, this is quite static - slowly changing dimension, unlikely to change very frequently.













**Creating S3 buckets and folder structures.**

Dimension table bucket and daily flight bucket have corresponding files.




Dimensional data pre-loaded loaded in s3




Under the daily flights folder, flight data records are loaded on a daily basis under a hive like partition and this file has 1 million records, we can break it down and make multiple parts if needed.


**Create Redshift tables and load the dimensional data to the dim\_table.**

Choose dc2.large node type if you are using a free trial with 2 nodes.

















Enable loading sample data





You can choose any of the options here for managing your password. I am choosing manual creation.




Attach the IAM role that is having all required permissions that are necessary for the functionality of redshift. I have one IAM role that is already created with all required policies.



Policies attached with IAM role for redshift.




And as you can see, I am selecting the default VPC and security group as I don't want to create a dedicated VPC since I am using a free trial.




And click on create









Redshift cluster is getting created





Before running the query, we should add the s3 gateway endpoint to the VPC where the redshift cluster is created. 

List of end points that needs to be created

Configure below mentioned vpc endpoints where our redshift is running





1. **Create s3 endpoint**



Select VPC where you want to add the s3 gateway endpoint.(This should be same VPC where your redshift cluster is running)



And create endpoint




In the VPC where redshift is running, apart from s3 endpoint for the redshift, you would also need to specify two more endpoints.So that your glue job is running even when you are orchestrating this.

Other two endpoints are.

Cloud watch monitoring endpoint

VPC access endpoint


Go to your AWS glue job, job details and check for the VPC, security group and subnet for the **redshift connection under the connections part** and create required endpoints for those VPC where you need to select the security group and subnet properly.


So make sure the monitoring end point and other endpoint that we are creating have the same subnet.



**12.Selecting the monitoring end point**




Selecting the same VPC , subnet and security group where redshift is hosted and this you can find out from the glue job details under the connections tab.

Select any two subnets available for high availability and fault tolerance.








**3.Do the same for glue end point as well**






Note: If redshift is using the default subnet, choose two subnets. Choosing multiple subnets in AWS is often done for high availability and fault tolerance


Also we would need to create an IAM role that will enable redshift to read data from s3 and load the data into redshift tables.

We would need to use this IAM role in redshift script to load data directly from s3 to redshift dimensional table




**Creating fact and dimensional table in redshift cluster.** 

Once the redshift cluster is created, run the DDL queries given in the file redshift\_create\_table\_commands.txt. Script will do below two processes.

1\.This will create a fact table named daily\_flights\_fact and dimensional  table.

2\.Will load the data stored in s3 into a dimensional table.














Created and loaded redshift dimensional table successfully with IAM role that we created 


Now create a fact table also, which will be our data landing table where the data analysis team will be doing their analysis.















Eventbridge rule should only respond to .csv file.The suffix pattern should match this.



3 crawlers and 3 metadata tables are needed.

1. One for dimensional redshift table - airport\_dimensional redshift table (almost Constant data)
1. One for s3  flights.csv data
1. One for flight\_fact redshift table.

First create two redshift tables



The fact table that we created is a denormalized table, where we have all details.

If we only ingest the s3 directly with some transformation into redshit into fact table with limited data, data analysis team woul need to write join to perform the analysis. So we denormalized the data before writing to redshift by combining dim and fact and load to redshift for ease of analysis by data analysis team. In DWH, we dont load denormalized table, they would need to perform join and this will slow down performance


Create jdbc connections in aws glue for redshift to use by crawler





Create redshift glue connection


















` `Create glue catalog database:



Create crawlers 




While creating crawler it will ask for path and u should give db name/schema name/table




Tables will look like this



3rd glue table created by crawler from source s3






Create glue etl



Source is raw data catalog  having one million records

Add a filter to only fetch those records where flight is delayed by a certain time. There is one column named depdelay in source.






This is how input data is looking like





This is how dimensional data looks like






We should write the data to the final table(fact table) in the below format.

CREATE TABLE airlines.daily\_flights\_fact (

`    `carrier VARCHAR(10),

`    `dep\_airport VARCHAR(200),

`    `arr\_airport VARCHAR(200),

`    `dep\_city VARCHAR(100),

`    `arr\_city VARCHAR(100),

`    `dep\_state VARCHAR(100),

`    `arr\_state VARCHAR(100),

`    `dep\_delay BIGINT,

`    `arr\_delay BIGINT

);

We need to fetch the complete details from the dimension table based on the OrginAirportID and DestAirportD. So we need to make join between these two data’s

We need to do two joins why?. This will be the actual SQL query to join and look up the dim table for airport complete details for deptairport id  and destination airport id from daily csv data.

SELECT

`    `flights.Carrier,

`    `flights.OriginAirportID,

`    `origin\_airport.city AS OriginCity,

`    `origin\_airport.state AS OriginState,

`    `origin\_airport.name AS OriginAirportName,

`    `flights.DestAirportID,

`    `dest\_airport.city AS DestCity,

`    `dest\_airport.state AS DestState,

`    `dest\_airport.name AS DestAirportName,

`    `flights.DepDelay,

`    `flights.ArrDelay

FROM

`    `flights

LEFT JOIN

`    `airports AS origin\_airport

ON

`    `flights.OriginAirportID = origin\_airport.airport\_id

LEFT JOIN

`    `airports AS dest\_airport

ON

`    `flights.DestAirportID = dest\_airport.airport\_id;


















Now select dimension data catalog table which is metadata table for redshift dimensional table


Role should the same role that is attached with the redshift










Temp s3 directory 



Now do the join operation:








Now we would like to keep the structure or data schema as that of the redshift fact table. So add a change schema.


We don't need date and orginairportid as orginairportid is already captured while joining and fetching from airportid and thereby fetching all details related with that id(we dont need any id of airport at all). Change the names of other columns to match with the final redshift column(fact table).Change the datatype as well in accordance with redshif table schema.

Join - join\_for\_dept\_airport






Now we can perform our next set of join- join\_for\_arrival\_port





Add change schema -  Modify arrival airport columns











Go to target - Add to redshift -add glue catalog



Specify temp bucket for redshift and IAM role associated with redshift.




Configure job details




Enable the job bookmarking for incremental load.



Save it.








Go to s3 source bucket and enable amazon eventbridge on





Step function:




Then select the choice state. This is similar to the if else condition.What ever the response of get crawler , inside that one there will be a parameter named crawler.state. Check if its running . if yes enter into wait state continue till it finishes run and move to next state where we are triggering the glue job(this is default)

Wait state: specify wait interval and after 5 seconds again go to get crawler




**Next glue job start state**

Add your glue job and enable the task to complete.

If the glue job is failing for some reason, we can add error handling, go to the error handling part and add error catch, **to send sns notification** as failed through SNS publish state.

**Please Note : This is before the choice state.**



Failure in glue jobs can happen at two places. First is when we are starting the glue job run , that time exception is happening and directly we will capture the exception and send the failed notification through SNS and this is what we configured in the above step. The second one will be after the job has been started and during some transformation operations in the glue job, the job failed for some reason, that needs to be captured and publish the failed notification through SNS again.We can make use of the same publish SNS state here. Also when the job is successful, that needs to be published to succeed SNS state. 

This can be configured through another choice state.

**Choice state:**

Wait for the glue job task to complete based on the toggle that we enabled(Wait for task to be complete) .

**If job status is SUCCEEDED** then the next state is SNS publish for Success.Else SNS state for failure.







So the jobRunState parameter will be there in response from the Glue StartJobRun state. This will be fetched once the Glue StartJobRun state(previous state) finishes its execution.And send to next SNS publish for success notification with below configurations where message is mentioned “Glue Job Execution is Successful !!”



If SUCCEED is not the case, we will send the failed notification with the state input message itself.(It have exception string)




Here we are calling the same SNS service for sending both the success and failure state with different messages for success and failure scenarios.

















**Create Event bridge Rule**







After making above mentioned configurations event pattern will be auto generated


In s3, we will be adding a new folder similar to hive like partition and that folder is also an object creation (uploading a s3 file is also an object creation).

What if we want to check whether it is triggered only when a csv file is uploaded. Add a suffix to the event pattern like this.



Add target in step function









