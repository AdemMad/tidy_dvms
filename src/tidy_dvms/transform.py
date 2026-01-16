from __future__ import annotations
import os
import warnings
from .transformers import transform_matchlineups
from .physical_splits import transform_physical_splits as ps_module
from .physical_total import transform_physical_total as pt_module
import xml.etree.ElementTree as ET
import polars as pl
import csv
import io
warnings.filterwarnings("ignore")


def transform_fixtures(df):

    df = df.select([
    'fixtureId', 'optaMatchId', 'optaCompetition', 
    'optaHomeTeamId', 'optaAwayTeamId', 'homeTeamName', 
    'awayTeamName','optaSeason', 'date', 'homeScore', 
    'awayScore', 'round'
    ])

    df = df.with_columns(
        pl.col("optaHomeTeamId").str.replace("t", ""),
        pl.col("optaAwayTeamId").str.replace("t", ""),
        pl.col("optaMatchId").str.replace("g", ""),
        pl.col("date").str.slice(0, 10).alias("date")
    )

    return df


def physical_splits(season_id, opta_competition_id, metadata_df, physical_splits_raw, opta_match_id, physical_splits):
    
    def read_csv(data: str) -> list:
        return [
            row
            for row in csv.reader(io.StringIO(data))
            if any(cell.strip() for cell in row)
        ]

    df_matchlineups = transform_matchlineups(
        metadata_df,
        table_name='match_lineups',
        sql_query='SELECT * FROM match_lineups'
    )

    ps_instance = ps_module.PhysicalSplit(physical_splits_raw, season_id, opta_competition_id, opta_match_id, df_matchlineups, physical_splits)

    splits_list = read_csv(physical_splits_raw)

    players_df, players_df_normalized, teams_df, teams_df_normalized = ps_instance.transform_physical_splits(splits_list, opta_match_id)

    return players_df, players_df_normalized, teams_df, teams_df_normalized



def physical_summary(df_fixtures, metadata_df, physical_summary_raw, opta_match_id):
    
    def read_physical_data(data: str):
        cleaned_data = []

        # If 'data' looks like a path to an existing file, open it
        if os.path.exists(data):
            f = open(data, "r")
        else:
            # Otherwise treat it as CSV text
            f = io.StringIO(data)

        with f:
            csvreader = csv.reader(f, delimiter=",")
            for row in csvreader:
                cleaned_data.append(row[0:24])

        return cleaned_data

    df_matchlineups = transform_matchlineups(
        metadata_df,
        table_name='match_lineups',
        sql_query='SELECT * FROM match_lineups'
    )

    cleaned_data = read_physical_data(physical_summary_raw)

    summary_df = pt_module.transform_physical_total(
        cleaned_data, df_fixtures, df_matchlineups, opta_match_id)

    return summary_df