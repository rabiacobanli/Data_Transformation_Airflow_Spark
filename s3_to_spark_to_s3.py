import findspark
findspark.init("/opt/spark")
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
.appName("final project") \
.master("local[2]") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
.config("spark.hadoop.fs.s3a.access.key", "root") \
.config("spark.hadoop.fs.s3a.secret.key", "root12345") \
.config("spark.hadoop.fs.s3a.path.style.access", True) \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.getOrCreate() 

credits_df = spark.read.parquet('s3a://tmdb-bronze/credits')

cast_schema = StructType([
            StructField("cast_id", IntegerType()),
            StructField("character", StringType()),
            StructField("credit_id", StringType()),
            StructField("gender", IntegerType()),
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("order", IntegerType())
])            
            
crew_schema = StructType([
            StructField("credit_id", StringType()),
            StructField("department", StringType()),
            StructField("gender", IntegerType()),
            StructField("id", IntegerType()),
            StructField("job", StringType()),
            StructField("name", StringType())
])     

credits_sch = credits_df.withColumn("cast", F.from_json(F.col("cast"), ArrayType( cast_schema ))) \
                        .withColumn("crew", F.from_json(F.col("crew"), ArrayType( crew_schema )))       
                        
cast_exp = credits_sch.select("movie_id", "title", F.explode_outer("cast").alias("cast"))

cast_cols = ['cast_id', 'character', 'credit_id', 'gender', 'id', 'name']

for col in cast_cols:
    cast_exp = cast_exp.withColumn(col, F.col("cast").getItem(col))
cast_df = cast_exp.na.fill("0000000000",["credit_id"])    

credits_cast = cast_df.drop('cast')                       

cast = credits_cast

crew_exp = credits_sch.select("movie_id", "title", F.explode_outer("crew").alias("crew"))

crew_cols = ['credit_id', 'department', 'gender', 'id', 'job', 'name']

for col in crew_cols:
    crew_exp = crew_exp.withColumn(col, F.col("crew").getItem(col))
crew_df = crew_exp.na.fill("0000000000",["credit_id"])  

credits_crew = crew_df.drop("crew")

crew = credits_crew

df_movies = spark.read.parquet('s3a://tmdb-bronze/movies')

select_movies = df_movies.withColumnRenamed("id","movie_id").select("movie_id","title","budget","homepage","original_language","original_title","overview","popularity","release_date","revenue","runtime","status","tagline","vote_average","vote_count")

movies_df = select_movies.withColumn("movie_id", F.col("movie_id").cast(StringType())) \
.withColumn("budget", F.col("budget").cast(DoubleType())) \
.withColumn("popularity", F.col("popularity").cast(FloatType())) \
.withColumn("release_date", F.to_date("release_date", "yyyy-MM-dd")) \
.withColumn("revenue", F.col("revenue").cast(DoubleType())) \
.withColumn("runtime", F.col("runtime").cast(IntegerType())) \
.withColumn("vote_average", F.col("vote_average").cast(FloatType())) \
.withColumn("vote_count", F.col("vote_count").cast(IntegerType()))

movies = movies_df

movies_tables = df_movies.withColumnRenamed("id","movie_id").select("movie_id","genres","keywords","production_companies","production_countries","spoken_languages")

schema = (ArrayType( StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType())])))
            
genres_exp = movies_tables.withColumn('movie_id',F.col("movie_id").cast(StringType())) \
                          .withColumn('genres',F.from_json(F.col('genres'),schema)) \
                          .select('movie_id',F.explode_outer('genres').alias('genres'))
                                      
cols = ["id","name"]

for col in cols:
    genres_exp = genres_exp.withColumn(col, F.col("genres").getItem(col))
genres = genres_exp.drop("genres").na.fill(value=-9999, subset=["id"])

keyword_exp = movies_tables.withColumn('movie_id', F.col("movie_id").cast(StringType())) \
                           .withColumn('keywords', F.from_json(F.col('keywords'),schema)) \
                           .select('movie_id', F.explode_outer('keywords').alias('keywords'))

for col in cols:
    keyword_exp = keyword_exp.withColumn(col, F.col("keywords").getItem(col))
keywords = keyword_exp.drop("keywords").na.fill(value=-9999, subset=["id"])

companies = movies_tables.withColumn('movie_id', F.col("movie_id").cast(StringType())) \
                         .withColumn('production_companies', F.from_json(F.col('production_companies'),schema)) \
                         .select('movie_id', F.explode_outer('production_companies').alias('production_companies'))

for col in cols:
    companies = companies.withColumn(col, F.col("production_companies").getItem(col))
companies = companies.drop("production_companies").na.fill(value=-9999, subset=["id"])

production_companies = companies

countries_schema = ArrayType (StructType([
        StructField('iso_3166_1', StringType()), 
        StructField('name', StringType())]))

countries_exp = movies_tables.withColumn('movie_id', F.col("movie_id").cast(StringType())) \
                             .withColumn('production_countries',F.from_json(F.col('production_countries'),countries_schema)) \
                             .select('movie_id', F.explode_outer('production_countries').alias('production_countries'))

countries_cols = ["iso_3166_1", "name"]
for col in countries_cols:
    countries_exp = countries_exp.withColumn(col, F.col("production_countries").getItem(col))
countries = countries_exp.drop("production_countries").na.fill(value="XX", subset=["iso_3166_1"])

production_countries = countries

languages_schema = ArrayType (StructType([
    StructField('iso_639_1', StringType()), 
    StructField('name', StringType())]))

languages_exp = movies_tables.withColumn('movie_id', F.col("movie_id").cast(StringType())) \
                             .withColumn('spoken_languages',F.from_json(F.col('spoken_languages'), languages_schema)) \
                             .select('movie_id', F.explode_outer('spoken_languages').alias('spoken_languages'))

languages_cols = ["iso_639_1", "name"]
for col in languages_cols:
    languages_exp = languages_exp.withColumn(col, F.col("spoken_languages").getItem(col))
languages = languages_exp.drop("spoken_languages").na.fill(value="XX", subset=["iso_639_1"])

spoken_languages = languages

table_dict = {'cast': cast, 'crew': crew, 'movies': movies, 'genres': genres, 'keywords': keywords, 'production_companies': production_companies, 'production_countries': production_countries, 'spoken_languages': spoken_languages}

for table_name, table in table_dict.items():
    deltaPath = f"s3a://tmdb-silver/{table_name}"
    
    table.write \
    .mode("overwrite") \
    .format("delta") \
    .save(deltaPath)






















 