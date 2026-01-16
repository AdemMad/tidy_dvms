import polars as pl

def transform_tracking(path):
    df_parquet = pl.read_parquet(path)

    # Home
    home_df = df_parquet.explode('homePlayers').unnest('homePlayers').drop(['awayPlayers', 'ball']).with_columns([
        pl.col("xyz").map_elements(lambda lst: lst[0]).alias("x"),
        pl.col("xyz").map_elements(lambda lst: lst[1]).alias("y"),
        pl.col("xyz").map_elements(lambda lst: lst[2]).alias("z"),
    ]).drop("xyz")

    # Away
    away_df = df_parquet.explode('awayPlayers').unnest('awayPlayers').drop(['homePlayers', 'ball']).with_columns([
        pl.col("xyz").map_elements(lambda lst: lst[0]).alias("x"),
        pl.col("xyz").map_elements(lambda lst: lst[1]).alias("y"),
        pl.col("xyz").map_elements(lambda lst: lst[2]).alias("z"),
    ]).drop("xyz")

    # Ball
    ball_df = ''

    union_df = pl.concat([home_df, away_df], how="vertical")

    
    return union_df
