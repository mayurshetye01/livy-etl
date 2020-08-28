from pyspark.sql import SparkSession


class SampleETL:
    # TODO-> Use file from livy 'files' option
    input_file = "/season-1819.csv"
    output_file = "/man_united_games.csv"

    spark = SparkSession.builder.getOrCreate()
    # Create the DataFrame
    df = spark.read.format('csv').option("header", "true").load(input_file)

    filtered_df = df.filter(df['HomeTeam'] == 'Man United')

    filtered_df.createOrReplaceTempView('man_utd_games')

    result_df = spark.sql('select '
                               'HomeTeam as home, AwayTeam as away, '
                               'FTHG as goals_for, FTAG as goals_against '
                               'from man_utd_games')
    result_df.write.csv(output_file)

    result_df.show()
