from pyspark.sql import SparkSession


class SampleETL:
    # TODO-> Use file from livy 'files' option
    input_file = "/season-1819.csv"
    output_file = "/man_united_games.csv"

    spark = SparkSession.builder.getOrCreate()

    # Create the DataFrame
    df = spark.read.option("header", "true").csv(input_file)

    # TODO -> Try RDD transformations

    filtered_df = df.filter((df['HomeTeam'] == 'Man United') | (df['AwayTeam'] == 'Man United'))

    filtered_df.createOrReplaceTempView('man_utd_games')

    result_df = spark.sql(
        '''SELECT HomeTeam as home, AwayTeam as away, FTHG as goals_for, FTAG as goals_against from man_utd_games''')
    result_df.write.mode('overwrite').csv(output_file)

    result_df.show()
