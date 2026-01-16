from __future__ import annotations
import pandas as pd
import duckdb
import polars as pl

def get_index_range(transform_dataframe, json_list):
    ''' 
    get index range of a player, team

    returns: 
        list (functions)
        dataframe list (dfs)
    '''

    from_dataframe = []
    to_dataframe = []
    final_dataframe_ix = []

    dfs = []

    # Get start ix of frame
    for from_row in range(10, 500, 12): 
        from_dataframe.append(from_row)

    # Get end ix of frame
    for to_row in range(21, 500, 12):
        to_dataframe.append(to_row)

    for jump_ix_by in range(9, 500, 12):
        final_dataframe_ix.append(jump_ix_by)

    functions = []

    # Declare arguments (for each player/team dataframe)
    for start_index, end_index, jump_index_by in zip(from_dataframe, to_dataframe, final_dataframe_ix):  
        function = lambda json_list=json_list, s=start_index, e=end_index, j=jump_index_by: transform_dataframe(json_list, s, e, j)
        functions.append(function)

    # Get frames from data source in chunks (minimum: 24)
    for frames in range(0, 24):     
        df = functions[frames](json_list)
        dfs.append(df)

    return dfs, functions



def capture_player_frame(list_count, dfs, functions):

    # Dataframe Concatenation
    print('Concatenate Dataframes...')

    if list_count == 323:
        df = pd.concat(dfs)

    elif list_count == 335:
        df = pd.concat(dfs + [functions[25]()])

    elif list_count == 347:
        df = pd.concat(dfs + [functions[25](), functions[26]()])

    elif list_count == 359:
        df = pd.concat(dfs + [functions[25](), functions[26](), functions[27]()])

    elif list_count == 371:
        df = pd.concat(dfs + [functions[25](), functions[26](), functions[27](), functions[28]()])

    elif list_count == 383:
        df = pd.concat(dfs + [functions[25](), functions[26](), functions[27](), functions[28](), functions[29]()])

    elif list_count == 395:
        df = pd.concat(dfs + [functions[25](), functions[26](), functions[27](), functions[28](), functions[29](), functions[30]()])

    elif list_count == 407:
        df = pd.concat(dfs + [functions[25](), functions[26](), functions[27](), functions[28](), functions[29](), functions[30](), functions[31]()])

    elif list_count == 419:
        df = pd.concat(dfs + [functions[25](), functions[26](), functions[27](), functions[28](), functions[29](), functions[30](), functions[31](), functions[32]()])

    elif list_count == 431:
        df = pd.concat(dfs + [functions[25](), functions[26](), functions[27](), functions[28](), functions[29](), functions[30](), functions[31](), functions[32](), functions[33]()])

    elif list_count == 443:
        df = pd.concat(dfs + [functions[25](), functions[26](), functions[27](), functions[28](), functions[29](), functions[30](), functions[31](), functions[32](), functions[33](), functions[34]()])

    print('Dataframe Concatenation Complete!')

    return df


def transform_matchlineups(df, table_name, sql_query):

    # Rename columns
    df = df.rename({
        "ssiId": "ssiIdd",
        "optaId": "optaMatchId"
    }).drop('optaUuid')

    # Home players
    df_home = df.explode("homePlayers").unnest("homePlayers").drop(['awayPlayers'])

    # Add team id to home players
    df_home = df_home.with_columns([
        pl.lit(df_home['homeOptaId']).alias("optaTeamId")
    ])

    # Away players
    df_away = df.explode("awayPlayers").unnest("awayPlayers").drop(['homePlayers'])

    # Add team id to away players
    df_away = df_away.with_columns([
        pl.lit(df_away['awayOptaId']).alias("optaTeamId")
    ])

    # combine home players + away players
    union_df = pl.concat([df_home, df_away])

    # Convert to DuckDB
    con = duckdb.connect()

    # Register the Polars DataFrame directly as a DuckDB view
    con.register(table_name, union_df.to_arrow())

    # Query the view
    final_df = con.execute(sql_query).fetchdf()

    return final_df


def get_halves(min_headers):
    """
    Generate half labels based on blank splits in the min_headers list.
    
    Returns:
        A list of '1', '2', '3', '4' depending on the number of blank splits found.
    """
    space_indices = [i for i, val in enumerate(min_headers) if str(val).strip() == '']

    if len(space_indices) == 1:
        halves = (
            ['1'] * space_indices[0] +
            ['2'] * (len(min_headers) - space_indices[0] - 1)
        )

    elif len(space_indices) == 2:
        halves = (
            ['1'] * space_indices[0] +
            ['2'] * (space_indices[1] - space_indices[0] - 1) +
            ['3'] * (len(min_headers) - space_indices[1] - 1)
        )

    elif len(space_indices) == 3:
        halves = (
            ['1'] * space_indices[0] +
            ['2'] * (space_indices[1] - space_indices[0] - 1) +
            ['3'] * (space_indices[2] - space_indices[1] - 1) +
            ['4'] * (len(min_headers) - space_indices[2] - 1)
        )

    else:
        raise ValueError(f"Unexpected number of blank splits: {len(space_indices)}")

    return halves

