from pyspark.sql import SparkSession


class SampleETL:
    # TODO-> Use file from livy 'files' option
    input_file = "/season-1819.csv"
    output_file = "/code/man_united_games.csv"

    spark = SparkSession.builder.getOrCreate()
    # Create the DataFrame
    df = spark.read.format('csv').option("header", "true").load(input_file)

    df.show()
