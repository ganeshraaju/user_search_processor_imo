
import sys
#import os
#import json
#import pyspark
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#import pyspark.sql.functions as func
import datetime as dt
from datetime import date, timedelta
import datetime

from datetime import datetime
from dateutil.relativedelta import relativedelta


from pyspark.sql.functions import lit
import pyspark.sql.functions as func
from pyspark.sql.functions import col

'''


'''
class SearchRequestProcessor:

    #The Init method to initialize the variables
    def __init__(self):
        self._debug_flag = False
        self.spark = SparkSession \
                        .builder \
                        .appName('SearchRequestProcessor') \
                        .config("spark.sql.sources.partitionOverwriteMode","dynamic")\
                        .getOrCreate()
        #self.spark.sparkContext._conf.set("spark.sql.parquet.mergeSchema", "true")
        self.spark._jsc.hadoopConfiguration().set('fs.s3.canned.acl', 'BucketOwnerFullControl')
        self.glueContext = GlueContext(self.spark.sparkContext)
        self._logger = self.glueContext.get_logger()
        self.write_log("*** Logging enabled ***")

        self.output_path = ""
        self.start_date = ""
        self.end_date = ""

        self.df_autosuggest_data = None
        self.df_user_search_data = None


    #This method is used for DEBUG purposes where we enable the flag to see the ERRORS
    def enable_debug(self):
        self._debug_flag = True
        self.write_log("Debug mode enabled")

    #This method checks whether the Spark dataframe is empty or not
    def check_for_empty_dataset(self, df):
        if df.rdd.isEmpty():

            message = "{} dataset is empty for the date range {} to {}. Throwing Exception !!!!". \
                        format(dataset_name, self.start_date, self.end_date)

            job.write_log(message, error=True)

            raise Exception("{} dataset is empty for the date range {} to {}.". \
                    format(dataset_name, self.start_date, self.end_date))

    #This method is invoked to write any log content
    def write_log(self, msg, error=False):
        if msg is not None:
            out = str(msg)
            if not error:
                self._logger.info("[User Search Analytics] {}".format(out))
            else:
                self._logger.error("[User Search Analytics] {}".format(out))


    #If start date is entered, use that date.
    #If start date is not entered, then use today's date
    def get_start_and_end_dates(self, args):
        start_date = args['start_date']
        end_date = args['end_date']

        current_date_as_string = datetime.today().strftime("%Y%m%d")
        current_date_as_datetime = datetime.strptime(current_date_as_string, "%Y%m%d")

        if not start_date.strip():
            start_date = current_date_as_datetime
            start_date = start_date.strftime("%Y%m%d")
        else:
            start_date = start_date.strip()

        if not end_date.strip():
            #end_date = current_date_as_datetime - timedelta(days=1)
            end_date = current_date_as_datetime
            end_date = end_date.strftime("%Y%m%d")
            #end_date = datetime.today().strftime("%Y%m%d")
        else:
            end_date = end_date.strip()

        self.write_log("Start Date: {}, End Date: {}".format(start_date, end_date))
        return start_date, end_date

    #This method writes the spark dataframe output in parquet format to S3
    def write_output_in_parquet(self, df):
        # Writes final dataframe to specified output path
        try:
            self.write_log("User Search Request Processor Output")
            df.write.parquet(self.output_path, mode='overwrite', partitionBy=["service_month"])
            self.write_log("User search analytics data written successfully to output")
        except Exception as ex:
            self.write_log(str(ex), True)
            raise RuntimeError("Error while writing the data to output path {}. Exception is {}".format(self.output_path, str(ex)))

    def calc_user_search_with_autosuggest(self):

        self.df_autosuggest_data.cache()
        self.df_user_search_data = self.df_user_search_data.join(broadcast(self.df_autosuggest_data),
                                                                 (self.df_user_search_data.user_search_phrase = self.df_autosuggest_data.autosuggest_phrase),
                                                                 how='left_outer')
        self.df_user_search_data = self.df_user_search_data.withColumn("search_phrase",coalesce(self.df_user_search_data["autosuggest_phrase"],self.df_user_search_data["user_search_phrase"]))
        self.df_user_search_data = self.df_user_search_data.withColumn("auto_suggested_ind",when(self.df_user_search_data.autosuggest_id.isNull(),"N").otherwise("Y"))
        self.df_user_search_data = self.df_user_search_data.withColumn("request_dt",to_date(col("request_timestamp"),"ddMMYYYY"))
        #Calculate Total Search count and Percentage autosuggested
        tot_count = self.df_user_search_data.count()
        df_percent_cnt = self.df_user_search_data
        autosuggest_count = df_percent_cnt.select().where(df_percent_cnt.auto_suggested_ind=='Y').count()
        percent_autosuggest = round((autosuggest_count/tot_count)*100,2)
        #added calculated columns to the dataframe
        self.df_user_search_data = self.df_user_search_data.withColumn("total_search_count",tot_count)
        self.df_user_search_data = self.df_user_search_data.withColumn("Autosuggest_percentage",percent_autosuggest)
        # Drop columns not required for final dataset
        self.df_user_search_data = self.df_user_search_data.drop("autosuggest_phrase").\
                                                            drop("user_search_phrase").\
                                                            drop("request_timestamp").\
                                                            drop("auto_suggested_ind").\
                                                            drop("search_phrase")

    def get_input_datasets(self, args):
        self.output_path = args["output_path"]
        self.start_date, self.end_date = self.get_start_and_end_dates(args)
        self.get_autosuggest_data(args)
        self.get_user_search_data(args)

    def get_autosuggest_data(self,args):
        dyf_autosuggest_data = self.glueContext.create_dynamic_frame_from_catalog(
                                                                                database = args["autosuggest_db"],
                                                                                table_name = args["autosuggest_table"])

        self.df_autosuggest_data = dyf_autosuggest_data.toDF()
        self.df_autosuggest_data = self.df_autosuggest_data.select("autosuggest_id","autosuggest_phrase")

    def get_user_search_data(self,args):
        predicate = "(request_timestamp >= '" + str(self.start_date) + "' and request_timestamp <= '" + str(self.end_date) + "')"
        self.write_log("Predicate: {} ".format(predicate))
        dyf_user_search_data = self.glueContext.create_dynamic_frame_from_catalog(
                                                                                database = args["user_search_db"],
                                                                                table_name = args["user_search_data_table"],
                                                                                push_down_predicate=predicate)


        self.df_user_search_data = dyf_user_search_data.toDF()
        self.df_user_search_data = self.df_user_search_data.select("user_search_phrase","request_timestamp").distinct()
        self.write_log("********************** Unique Search Request for Date Range from {} till {} : {}".format(str(self.start_date),str(self.end_date), str(self.df_user_search_data.count())))

    #Main method to call all functions for User Search request Analytics
    def process(self, args):

        try:
            self.get_input_datasets(args)
            self.calc_user_search_with_autosuggest()
            df_user_search_results = self.df_user_search_data
            self.check_for_empty_dataset(df_user_search_results)
            self.write_output_in_parquet(df_user_search_results)

        except Exception as ex:
            self.write_log(str(ex), True)
            raise Exception("Exception Occurred in process method. Message : {}".format(str(ex)))
        finally:
            try:
                if(self.spark):
                    self.spark.stop()
                    self.write_log("Spark Session closed successfully")
            except Exception as ex:
                self.write_log(str(ex), True)

if __name__ == "__main__":
    # Get Command line arguments

    job_args = getResolvedOptions(sys.argv,
                                          ['autosuggest_db',
                                           'autosuggest_table',
                                           'user_search_db',
                                           'user_search_data_table',
                                           'start_date',
                                           'end_date'
                                           ])

    job = SearchRequestProcessor()
    job.write_log("Job Parameters: {}".format(str(job_args)))
    try:
        if job_args['debug_flag'] == 'Y':
            job.enable_debug()
        job.process(job_args)
        job.write_log("User Search Request processor job execution successfully completed")
    except Exception as ex:
        job.write_log(str(ex), error=True)
        job.write_log("User Search Request processor job failed")
        sys.exit(1)
