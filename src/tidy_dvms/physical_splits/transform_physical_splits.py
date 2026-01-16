from __future__ import annotations
import pandas as pd
# from ..transformers import get_index_range, capture_player_frame, get_halves

from tidy_dvms.transformers import get_index_range, capture_player_frame, get_halves
import duckdb
import polars as pl


class PhysicalSplit:
    def __init__(self, data_list, season_id, opta_compid, opta_matchid, df_matchlineups, df_fixtures):
        self.season_id = season_id
        self.opta_compid = opta_compid
        self.opta_matchid = opta_matchid
        self.bronze_path = data_list
        self.df_matchlineups = df_matchlineups
        self.df_fixtures = df_fixtures

    def transform_dataframe(self, data_list, row_1: int, row_2: int, player_inx: int, file_name: str):
        self.list_count = len(data_list)

        # Extract Minute headers (from row 9, skipping first column)
        min_headers = data_list[9][1:]
        # Remove any row that contains 'Minute Splits'
        data_list = [row for row in data_list if 'Minute Splits' not in row]

        # Extract relevant rows from the cleaned data list
        data = data_list[row_1:row_2]

        # Determine ID types based on match ID format
        match_id = data_list[2][0]
        self.player_id = "ssiId" if len(match_id) > 10 else "OptaPlayerId"
        self.team_col = "SsiId" if len(match_id) > 10 else "OptaId"

        # Extract match date from the second row
        fixture = data_list[1][0].split(' : ')[0]
        match_date = data_list[1][0].split(' : ')[1]

        # Create DataFrame and set headers
        df = pd.DataFrame(data).transpose()
        df.columns = df.iloc[0]
        df = df.iloc[1:]

        # Get halves for periods
        half = get_halves(min_headers)
        min_headers = [x for x in min_headers if x.strip()]  # Clean up headers

        # Filter out rows with empty 'Total Distance'
        df = df[df['Total Distance'] != '']
        df['Period'] = half
        df['Minute'] = min_headers

        # Extract player ID from player line
        player_info = data_list[player_inx][0].split('(')
        player_id = player_info[1].replace(')', '')

        # Add columns to df
        df['Player ID'] = player_id
        df['Fixture ID'] = file_name
        df['Fixture'] = fixture
        df['Match Date'] = match_date

        return df

    def transform_physical_splits(self, data_list, opta_matchid):
        # Generate transformed player frames and associated functions
        dfs, functions = get_index_range(
            lambda dl, r1, r2, pi: self.transform_dataframe(dl, r1, r2, pi, opta_matchid),
            data_list
        )
        df = capture_player_frame(self.list_count, dfs, functions)

        # Connect to DuckDB in-memory DB
        conn = duckdb.connect()

        # Register tables in DuckDB
        conn.register('physical_splits', df)
        conn.register('fixtures', self.df_fixtures)
        conn.register('matchlineups', self.df_matchlineups)

        # Query players data
        players_df_normalized = conn.execute(f'''
            SELECT 
                "Fixture ID" AS OptaMatchId, 
                REPLACE(ml.OptaPlayerId, 'Unknown opta', '0') AS "OptaPlayerId",
                ml.OptaTeamId,
                Minute, 
                REPLACE(Period, '', '1') AS Period, 
                "Total Distance" AS TotalDistance, 
                "Walking Distance" AS WalkingDistance, 
                "Jogging Distance" AS JoggingDistance, 
                "Low Speed Running Distance" AS LowSpeedRunningDistance, 
                "High Speed Running Distance" AS HighSpeedRunningDistance, 
                "Sprinting Distance" AS SprintingDistance, 
                "Walking Count" AS WalkingCount, 
                "Jogging Count" AS JoggingCount, 
                "Low Speed Running Count" AS LowSpeedRunningCount, 
                "High Speed Running Count" AS HighSpeedRunningCount, 
                "Sprinting Count" AS SprintingCount
            FROM physical_splits ps
            JOIN (SELECT OptaId AS OptaPlayerId, ssiId, optaTeamId FROM matchlineups) AS ml
                ON ps."Player ID" = ml.{self.player_id}
            WHERE "Player ID" NOT IN (
                SELECT home{self.team_col} AS teamid FROM matchlineups
                UNION 
                SELECT away{self.team_col} AS teamid FROM matchlineups
            )
        ''').df()


        players_df = conn.execute(f'''
            SELECT 
                Fixture,
                ps."Match Date" AS MatchDate,
                UPPER(ml.name) AS PlayerName,
                ml.number AS PlayerNumber,
                ml.Position AS Position,
                UPPER(f.TeamName) as TeamName,
                UPPER(f.Side) AS Side,
                Minute, 
                REPLACE(Period, '', '1') AS Period, 
                "Total Distance" AS TotalDistance, 
                "Walking Distance" AS WalkingDistance, 
                "Jogging Distance" AS JoggingDistance, 
                "Low Speed Running Distance" AS LowSpeedRunningDistance, 
                "High Speed Running Distance" AS HighSpeedRunningDistance, 
                "Sprinting Distance" AS SprintingDistance, 
                "Walking Count" AS WalkingCount, 
                "Jogging Count" AS JoggingCount, 
                "Low Speed Running Count" AS LowSpeedRunningCount, 
                "High Speed Running Count" AS HighSpeedRunningCount, 
                "Sprinting Count" AS SprintingCount
            FROM physical_splits ps
            JOIN (SELECT OptaId AS OptaPlayerId, ssiId, optaTeamId, periods, name, number, position FROM matchlineups) AS ml
                ON ps."Player ID" = ml.{self.player_id}
            JOIN (
                SELECT fixtureId, OptaMatchId, OptaHomeTeamId AS TeamId, homeTeamName AS TeamName, 'Home' AS Side
                FROM fixtures WHERE optaMatchId = {opta_matchid}
                UNION
                SELECT fixtureId, OptaMatchId, OptaAwayTeamId AS TeamId, awayTeamName AS TeamName, 'Away' AS Side
                FROM fixtures WHERE optaMatchId = {opta_matchid}
            ) AS f ON f.OptaMatchId = ps."Fixture ID" AND ml.OptaTeamId=f.TeamId
            WHERE "Player ID" NOT IN (
                SELECT home{self.team_col} AS teamid FROM matchlineups
                UNION 
                SELECT away{self.team_col} AS teamid FROM matchlineups
            )
        ''').df()

        # Query teams data
        teams_df_normalized = conn.execute(f'''
            SELECT 
                "Fixture ID" AS OptaMatchId, 
                ml.OptaTeamId, 
                Minute, 
                Period, 
                "Total Distance" AS TotalDistance, 
                "Walking Distance" AS WalkingDistance, 
                "Jogging Distance" AS JoggingDistance, 
                "Low Speed Running Distance" AS LowSpeedRunningDistance, 
                "High Speed Running Distance" AS HighSpeedRunningDistance, 
                "Sprinting Distance" AS SprintingDistance, 
                "Walking Count" AS WalkingCount, 
                "Jogging Count" AS JoggingCount, 
                "Low Speed Running Count" AS LowSpeedRunningCount, 
                "High Speed Running Count" AS HighSpeedRunningCount, 
                "Sprinting Count" AS SprintingCount
            FROM physical_splits ps
            JOIN (
                SELECT home{self.team_col} AS teamid, homeOptaId AS OptaTeamId FROM MatchLineups
                UNION
                SELECT away{self.team_col} AS teamid, awayOptaId AS OptaTeamId FROM MatchLineups
            ) AS ml ON ps."Player ID" = ml.teamid
        ''').df()

        # Query teams data
        teams_df = conn.execute(f'''
            SELECT 
                Fixture, 
                ps."Match Date" AS MatchDate,
                UPPER(f.TeamName) as TeamName,
                Minute, 
                Period, 
                "Total Distance" AS TotalDistance, 
                "Walking Distance" AS WalkingDistance, 
                "Jogging Distance" AS JoggingDistance, 
                "Low Speed Running Distance" AS LowSpeedRunningDistance, 
                "High Speed Running Distance" AS HighSpeedRunningDistance, 
                "Sprinting Distance" AS SprintingDistance, 
                "Walking Count" AS WalkingCount, 
                "Jogging Count" AS JoggingCount, 
                "Low Speed Running Count" AS LowSpeedRunningCount, 
                "High Speed Running Count" AS HighSpeedRunningCount, 
                "Sprinting Count" AS SprintingCount
            FROM physical_splits ps
            JOIN (
                SELECT home{self.team_col} AS teamid, homeOptaId AS OptaTeamId FROM MatchLineups
                UNION
                SELECT away{self.team_col} AS teamid, awayOptaId AS OptaTeamId FROM MatchLineups
            ) AS ml ON ps."Player ID" = ml.teamid
            JOIN (
                SELECT fixtureId, OptaMatchId, OptaHomeTeamId AS TeamId, homeTeamName AS TeamName, 'Home' AS Side
                FROM fixtures WHERE optaMatchId = {opta_matchid}
                UNION
                SELECT fixtureId, OptaMatchId, OptaAwayTeamId AS TeamId, awayTeamName AS TeamName, 'Away' AS Side
                FROM fixtures WHERE optaMatchId = {opta_matchid}
            ) AS f ON f.OptaMatchId = ps."Fixture ID" AND ml.OptaTeamId=f.TeamId
        ''').df()


        return players_df, players_df_normalized, teams_df, teams_df_normalized
    