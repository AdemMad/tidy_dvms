from __future__ import annotations
import pandas as pd
import duckdb
import polars as pl


def transform_physical_total(cleaned_data, df_fixtures, df_matchlineups, opta_matchid) -> None:

    data = cleaned_data[10:]

    # Get headers   
    headers = cleaned_data[9]

    if len(headers) == 24:

        # Get Totals
        # Game Time
        game_time_row = cleaned_data[4]

        total_game_time = game_time_row[2]
        first_half_time = game_time_row[3]
        second_half_time = game_time_row[4]

        # Home EPT Time
        home_ept_row = cleaned_data[6]
        
        home_ept_total = home_ept_row[2]
        home_ept_fh = home_ept_row[3]
        home_ept_sh = home_ept_row[4]

        # Away EPT Time
        away_ept_row = cleaned_data[7]
        
        away_ept_total = away_ept_row[2]
        away_ept_fh = away_ept_row[3]
        away_ept_sh = away_ept_row[4]


        # Execute the SQL query to select all data from the view
        df = pd.DataFrame(data=data, columns=headers)
        df = df[df['Player'] != 'Player']

        # Create a DuckDB connection and register the DataFrame as a view
        conn = duckdb.connect()

        # Register DataFrames as a view
        conn.register('physical_total', df)
        conn.register('fixtures', df_fixtures)
        conn.register('matchlineups', df_matchlineups)

        # Get PlayerId (identify Opta or SS Player Id)
        match_id = cleaned_data[1][1].split(': ')[1]
        player_id = "ssiId" if len(match_id) > 10 else "OptaPlayerId"

        # Join with Lineups view to get OptaTeamIds
        conn.execute(f''' 
            CREATE VIEW physical_total_1 AS
            SELECT 
                {opta_matchid} AS OptaGameId,      
                ml.OptaPlayerId, "Player", "Minutes", "Distance", "Walking",
                "Jogging", "Running", "High Speed Running", "Sprinting",
                "No. of High Intensity Runs", "Top Speed", "Average Speed",
                "Distance TIP", "HSR Distance TIP", "Sprint Distance TIP",
                "No. of High Intensity Runs TIP", "Distance OTIP", "HSR Distance OTIP",
                "Sprint Distance OTIP", "No. of High Intensity Runs OTIP",
                "Distance BOP", "HSR Distance BOP", "Sprint Distance BOP",
                "No. of High Intensity Runs BOP"
                ,ml.OptaTeamId
            FROM physical_total pt
            JOIN (SELECT OptaId AS OptaPlayerId, ssiId, optaTeamId FROM matchlineups) AS ml ON pt.ID = ml.{player_id}
            ''') 

        # Create Home/Away side
        conn.execute(f'''
            CREATE VIEW physical_total_2 AS
            SELECT 
                pt."OptaGameId", OptaPlayerId, "OptaTeamId" AS OptaTeamId, f."Side", 
                "Minutes", "Distance", "Walking", "Jogging", "Running", "High Speed Running" AS HighSpeedRunning, 
                "Sprinting", "No. of High Intensity Runs" AS HighIntensityRuns, "Top Speed" AS TopSpeed, 
                "Average Speed" AS AverageSpeed, "Distance TIP" AS DistanceTIP, "HSR Distance TIP" AS HSRDistanceTIP, 
                "Sprint Distance TIP" AS SprintDistanceTIP, "No. of High Intensity Runs TIP" AS HighIntensityRunsTIP, 
                "Distance OTIP" AS DistanceOTIP, "HSR Distance OTIP" AS HSRDistanceOTIP, "Sprint Distance OTIP" AS SprintDistanceOTIP, 
                "No. of High Intensity Runs OTIP" AS HighIntensityRunsOTIP, "Distance BOP" AS DistanceBOP, 
                "HSR Distance BOP" AS HSRDistanceBOP, "Sprint Distance BOP" AS SprintDistanceBOP, 
                "No. of High Intensity Runs BOP" AS HighIntensityRunsBOP
            FROM physical_total_1 pt
            JOIN 
            (
                SELECT fixtureId, OptaGameId, OptaHomeTeamId AS TeamId, 'Home' AS Side
                FROM fixtures WHERE OptaGameId = {opta_matchid}
                UNION
                SELECT fixtureId, OptaGameId, OptaAwayTeamId AS TeamId, 'Away' AS Side
                FROM fixtures WHERE OptaGameId = {opta_matchid}
            ) AS f ON f.OptaGameId = pt.OptaGameId AND pt.OptaTeamId=f.TeamId
            ''')
        
        # Combine Home + Away team
        conn.execute(f'''
            CREATE VIEW final_table AS
            SELECT 
                *, '{home_ept_fh}' AS EPTFirstHalf, '{home_ept_sh}' AS EPTSecondHalf, '{home_ept_total}' AS EPTTotal
                FROM physical_total_2
                WHERE Side = 'Home'
                UNION
                SELECT *, '{away_ept_fh}' AS EPTFirstHalf, '{away_ept_sh}' AS EPTSecondHalf, '{away_ept_total}' AS EPTTotal
            FROM physical_total_2
            WHERE Side = 'Away'
            ''')

        # final df
        final_df = conn.execute(f'''
            SELECT  
                OptaGameId, REPLACE("OptaPlayerId", 'Unknown opta', '0') AS "OptaPlayerId", 
                OptaTeamId, Minutes, Distance, Walking, Jogging, Running, HighSpeedRunning, 
                Sprinting, HighIntensityRuns, TopSpeed, AverageSpeed, DistanceTIP, HSRDistanceTIP, 
                SprintDistanceTIP, HighIntensityRunsTIP, DistanceOTIP, HSRDistanceOTIP, 
                SprintDistanceOTIP, HighIntensityRunsOTIP, DistanceBOP, HSRDistanceBOP, 
                SprintDistanceBOP, HighIntensityRunsBOP, EPTFirstHalf, EPTSecondHalf, EPTTotal,
                '{first_half_time}' AS FHTime, '{second_half_time}' AS SHTime, '{total_game_time}' AS TotalGameTime
            FROM final_table
            ''').pl()

        final_df = final_df.to_pandas()

        return final_df
