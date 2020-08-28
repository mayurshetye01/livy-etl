from pyspark.sql import SparkSession


class SampleETL:
    input_file = "https://datahub.io/sports-data/english-premier-league/r/season-1819.csv"
    output_file = "/code/man_united_games.csv"

    spark = SparkSession.builder.getOrCreate()
    # Create the DataFrame
    df = spark.read.csv(input_file, header=True)

    filtered_df = df.filter(df['HomeTeam'] == 'Man United' or df['AwayTeam'] == 'Man United')
    filtered_df.show()

    filtered_df.createOrReplaceTempView('man_utd_games')

    result_df = spark.sql('select '
                               'HomeTeam as home, AwayTeam as away, '
                               'FTHG as goals_for, FTAG as goals_against'
                               'from man_utd_games')
    result_df.write.csv(output_file)

    print('Data exported successfully')

    result_df.show()
