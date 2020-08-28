from pyspark.sql import SparkSession


class SampleETL:
    base_dir = "/code/"
    input_file = base_dir + "data.csv"
    output_file = base_dir + "man_united_games.csv"

    sqlContext = SparkSession.builder.getOrCreate()

    # Create the DataFrame
    df = sqlContext.read.csv(input_file)

    filtered_df = df.filter(df['HomeTeam'] == 'Man United' or df['AwayTeam'] == 'Man United')
    filtered_df.createOrReplaceTempView('man_utd_games')

    result_df = sqlContext.sql('select '
                               'HomeTeam as home, AwayTeam as away, '
                               'FTHG as goals_for, FTAG as goals_against'
                               'from man_utd_games')
    result_df.write.csv(output_file)

    print('Data exported successfully')

    result_df.show()
