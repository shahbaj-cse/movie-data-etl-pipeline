import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node S3 Data Source
S3DataSource_node1730789314232 = glueContext.create_dynamic_frame.from_catalog(database="movie_db", table_name="input", transformation_ctx="S3DataSource_node1730789314232")

# Script generated for node Data Quality Checks
DataQualityChecks_node1730789403682_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 8.5 and 10.3
    ]
"""

DataQualityChecks_node1730789403682 = EvaluateDataQuality().process_rows(frame=S3DataSource_node1730789314232, ruleset=DataQualityChecks_node1730789403682_ruleset, publishing_options={"dataQualityEvaluationContext": "DataQualityChecks_node1730789403682", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1730789555667 = SelectFromCollection.apply(dfc=DataQualityChecks_node1730789403682, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1730789555667")

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1730789563713 = SelectFromCollection.apply(dfc=DataQualityChecks_node1730789403682, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1730789563713")

# Script generated for node Conditional Router
ConditionalRouter_node1730790022102 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1730789563713,
  group_filters = [GroupFilter(name = "output_group_1", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node default_group
default_group_node1730790022366 = SelectFromCollection.apply(dfc=ConditionalRouter_node1730790022102, key="default_group", transformation_ctx="default_group_node1730790022366")

# Script generated for node output_group_1
output_group_1_node1730790022419 = SelectFromCollection.apply(dfc=ConditionalRouter_node1730790022102, key="output_group_1", transformation_ctx="output_group_1_node1730790022419")

# Script generated for node drop_coloums
drop_coloums_node1730790464179 = ApplyMapping.apply(frame=default_group_node1730790022366, mappings=[("overview", "string", "overview", "string"), ("gross", "string", "gross", "decimal"), ("director", "string", "director", "string"), ("certificate", "string", "certificate", "string"), ("star4", "string", "star4", "string"), ("runtime", "string", "runtime", "string"), ("star2", "string", "star2", "string"), ("star3", "string", "star3", "string"), ("no_of_votes", "long", "no_of_votes", "int"), ("series_title", "string", "series_title", "string"), ("meta_score", "long", "meta_score", "int"), ("star1", "string", "star1", "string"), ("genre", "string", "genre", "string"), ("released_year", "string", "released_year", "int"), ("poster_link", "string", "poster_link", "string"), ("imdb_rating", "double", "imdb_rating", "decimal")], transformation_ctx="drop_coloums_node1730790464179")

# Script generated for node Amazon S3 ( Passed Result )
AmazonS3PassedResult_node1730789667922 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1730789555667, connection_type="s3", format="json", connection_options={"path": "s3://movie-data-buck/rule_outcome/", "partitionKeys": []}, transformation_ctx="AmazonS3PassedResult_node1730789667922")

# Script generated for node Amazon S3 ( Failed )
AmazonS3Failed_node1730790181401 = glueContext.write_dynamic_frame.from_options(frame=output_group_1_node1730790022419, connection_type="s3", format="json", connection_options={"path": "s3://movie-data-buck/bad_records/", "partitionKeys": []}, transformation_ctx="AmazonS3Failed_node1730790181401")

# Script generated for node Redshift_load
Redshift_load_node1730790708469 = glueContext.write_dynamic_frame.from_catalog(frame=drop_coloums_node1730790464179, database="movie_db", table_name="destination_dev_movies_movie_rating", redshift_tmp_dir="s3://movie-data-buck/temp/",additional_options={"aws_iam_role": "arn:aws:iam::940482409720:role/redshift-role"}, transformation_ctx="Redshift_load_node1730790708469")

job.commit()