from sqlalchemy import create_engine
import psycopg2
import os
import pandas as pd

# Link to Premier League Tables
PL_STATS_URL = r"https://fbref.com/en/comps/9/Premier-League-Stats"
PL_SCORES_FIXTURES_URL = (
    r"https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures"
)

# Database Configurations
database = os.environ["database"]
user = os.environ["user"]
password = os.environ["password"]
host = os.environ["host"]
port = os.environ["port"]

# database = "football-db"
# user = "user"
# password = "password"
# host = "192.168.59.101"
# port = "30432"


def clean_column_names(df):
    columns = []
    for each in df.columns:
        if "+/-" in each:
            each = each.replace("+/-", "_plus_minus")
        if "#" in each:
            each = each.replace("#", "num")
        if "/" in each:
            each = each.replace("/", "_per_")
        if "%" in each:
            each = each.replace("%", "_pct")
        if "-" in each:
            each = each.replace("-", "_")
        if ":" in each:
            each = each.replace(":", "_")
        if "+" in each:
            each = each.replace("+", "_and_")

        each = each.lower()

        columns.append(each)

    df.columns = columns

    if "squad" in df.columns:
        df["squad"] = df["squad"].str.lower()

    return df


def flatten_df(df):
    """
    Flattens the DataFrame

    Args:
        df (pd.DataFrame): DataFrame to Flatten

    Returns:
        pd.DataFrame: Returns Flattened DataFrame
    """
    try:
        # Flatten the dataframe by reducing the levels of the columns
        df.columns = [
            "_".join([each.strip().replace(" ", "") for each in i])
            if "Unnamed" not in i[0]
            else i[-1].strip().replace(" ", "_")
            for i in df.columns
        ]
    except Exception as e:
        # Error in Flattening the dataframe
        print("Error occurred unable to flatten the dataframe.")
        print(f"Error message: {str(e)}")
        raise (e)
    finally:
        # Return the DataFrame
        return df


def transform_combine(raw_squad_df, raw_opponent_df):
    """
    Transforms the DataFrames denoting squad and opponent stats into a single table.

    Args:
        raw_squad_df (pd.DataFrame): Squad Stats DataFrame
        raw_opponent_df (pd.DataFrame): Opponent Stats DataFrame

    Returns:
        pd.DataFrame: Appended Stats Dataframe containing stats of both squad and opponent stats.
    """
    try:
        # Squad Dataframe: Flatten and Creating the Value colum
        raw_squad_df = flatten_df(raw_squad_df.copy())
        raw_squad_df.loc[:, "Value"] = "squad"

        # Opponent Dataframe: Flatten, removing the string "VS", and Creating the Value colum
        raw_opponent_df = flatten_df(raw_opponent_df.copy())
        raw_opponent_df.loc[:, "Squad"] = raw_opponent_df.loc[:, "Squad"].apply(
            lambda x: " ".join(x.split()[1:])
        )
        raw_opponent_df.loc[:, "Value"] = "opponent"

        # Appending the dataframes
        stats_df = raw_opponent_df.append(raw_squad_df, ignore_index=True)

    except Exception as e:
        # Error in Flattening the dataframe
        print("Error occurred unable to append the dataframes.")
        print(f"Error message: {str(e)}")
        raise (e)

    finally:
        # Return the Appended DataFrame
        return stats_df


def pushToDB(table_name, df, conn, if_exists="replace", index=False):
    """
    Pushes the DataFrame to a specified table.

    Args:
        table_name (str): Name of the resulting table in database.
        df (pd.DataFrame): DataFrame to uploaded to database.
        conn (sqlalchemy.engine.base.Connection): Connection engine to the database.
        if_exists (str, optional): How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.

            Defaults to "replace".

        index (bool, optional): Write DataFrame index as a column with column name "index".

            Defaults to False.
    """

    df = clean_column_names(df.copy())

    # Begin a transaction
    transaction = conn.begin()

    try:
        # Push the DataFrame to PostgreSQL
        df.to_sql(name=table_name, con=conn, if_exists=if_exists, index=index)

        # Commit the transaction
        transaction.commit()
        print(f"{table_name} has been successfully committed.")

    except Exception as e:
        # Rollback the transaction if there's an error
        transaction.rollback()

        print(
            f"Error occurred in uploading {table_name}. Transaction has been rolled back."
        )
        print(f"Error message: {str(e)}")
        raise (e)


print("Data Extract Phase Started....")

raw_scores_and_fixtures = pd.read_html(PL_SCORES_FIXTURES_URL)[0]

all_tables = pd.read_html(PL_STATS_URL)

raw_regular_season_overall = all_tables[0]
raw_regular_season_home_away = all_tables[1]

raw_squad_standard_stats_squad = all_tables[2]
raw_squad_standard_stats_opponent = all_tables[3]

raw_squad_goalkeeping_squad = all_tables[4]
raw_squad_goalkeeping_opponent = all_tables[5]

raw_squad_advanced_goalkeeping_squad = all_tables[6]
raw_squad_advanced_goalkeeping_opponent = all_tables[7]

raw_squad_shooting_squad = all_tables[8]
raw_squad_shooting_opponent = all_tables[9]

raw_squad_passing_squad = all_tables[10]
raw_squad_passing_opponent = all_tables[11]

raw_squad_pass_types_squad = all_tables[12]
raw_squad_pass_types_opponent = all_tables[13]

raw_squad_goal_shot_creation_squad = all_tables[14]
raw_squad_goal_shot_creation_opponent = all_tables[15]

raw_squad_defensive_actions_squad = all_tables[16]
raw_squad_defensive_actions_opponent = all_tables[17]

raw_squad_possession_squad = all_tables[18]
raw_squad_possession_opponent = all_tables[19]

raw_squad_playing_time_squad = all_tables[20]
raw_squad_playing_time_opponent = all_tables[21]

raw_squad_miscellaneous_stats_squad = all_tables[22]
raw_squad_miscellaneous_stats_opponent = all_tables[23]

print("Data Extract Phase Ended....")

print("Data Transformation Phase Started....")

print("Regular Season Transformations....")
raw_regular_season_home_away = flatten_df(raw_regular_season_home_away.copy())
raw_regular_season_overall.columns = [
    "Overall_" + i.strip().replace(" ", "") if i not in ["Rk", "Squad"] else i
    for i in raw_regular_season_overall.columns
]
regular_season = raw_regular_season_overall.merge(
    right=raw_regular_season_home_away,
    how="inner",
    on=["Rk", "Squad"],
    validate="one_to_one",
)

print("Standard Stats Transformations....")
standard_stats = transform_combine(
    raw_squad_df=raw_squad_standard_stats_squad.copy(),
    raw_opponent_df=raw_squad_standard_stats_opponent.copy(),
)

print("Goalkeeping Stats Transformations....")
goalkeeping_stats = transform_combine(
    raw_squad_df=raw_squad_goalkeeping_squad.copy(),
    raw_opponent_df=raw_squad_goalkeeping_opponent.copy(),
)

print("Advanced Goalkeeping Stats Transformations....")
advanced_goalkeeping_stats = transform_combine(
    raw_squad_df=raw_squad_advanced_goalkeeping_squad.copy(),
    raw_opponent_df=raw_squad_advanced_goalkeeping_opponent.copy(),
)

print("Shooting Stats Transformations....")
shooting_stats = transform_combine(
    raw_squad_df=raw_squad_shooting_squad.copy(),
    raw_opponent_df=raw_squad_shooting_opponent.copy(),
)

print("Passing Stats Transformations....")
passing_stats = transform_combine(
    raw_squad_df=raw_squad_passing_squad.copy(),
    raw_opponent_df=raw_squad_passing_opponent.copy(),
)

print("Passing Types Stats Transformations....")
passing_types_stats = transform_combine(
    raw_squad_df=raw_squad_pass_types_squad.copy(),
    raw_opponent_df=raw_squad_pass_types_opponent.copy(),
)

print("Goal Shot Creation Stats Transformations....")
goal_shot_creation_stats = transform_combine(
    raw_squad_df=raw_squad_goal_shot_creation_squad.copy(),
    raw_opponent_df=raw_squad_goal_shot_creation_opponent.copy(),
)

print("Defensive Action Stats Transformations....")
defensive_action_stats = transform_combine(
    raw_squad_df=raw_squad_defensive_actions_squad.copy(),
    raw_opponent_df=raw_squad_defensive_actions_opponent.copy(),
)

print("Posession Stats Transformations....")
possession_stats = transform_combine(
    raw_squad_df=raw_squad_possession_squad.copy(),
    raw_opponent_df=raw_squad_possession_opponent.copy(),
)

print("Playing Time Stats Transformations....")
playing_time_stats = transform_combine(
    raw_squad_df=raw_squad_playing_time_squad.copy(),
    raw_opponent_df=raw_squad_playing_time_opponent.copy(),
)

print("Miscellaneous Stats Transformations....")
miscellaneous_stats = transform_combine(
    raw_squad_df=raw_squad_miscellaneous_stats_squad.copy(),
    raw_opponent_df=raw_squad_miscellaneous_stats_opponent.copy(),
)

print("Data Transformation Phase Ended....")

try:
    print("Establishing Connection with DB....")
    db = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    conn = db.connect()
    print("Successfully Established Connection with DB....")

except Exception as e:
    print("Unable to Establish Connection with DB....")
    raise (e)


print("Data Loading Phase Started....")

# Creating/Updating the regular_season table  in the football-db database
pushToDB(table_name="regular_season", df=regular_season, conn=conn)

# Creating/Updating the standard_stats table  in the football-db database
pushToDB(table_name="standard_stats", df=standard_stats, conn=conn)

# Creating/Updating the goalkeeping_stats table  in the football-db database
pushToDB(table_name="goalkeeping_stats", df=goalkeeping_stats, conn=conn)

# Creating/Updating the advanced_goalkeeping_stats table  in the football-db database
pushToDB(
    table_name="advanced_goalkeeping_stats", df=advanced_goalkeeping_stats, conn=conn
)

# Creating/Updating the shooting_stats table  in the football-db database
pushToDB(table_name="shooting_stats", df=shooting_stats, conn=conn)

# Creating/Updating the passing_stats table  in the football-db database
pushToDB(table_name="passing_stats", df=passing_stats, conn=conn)

# Creating/Updating the passing_types_stats table  in the football-db database
pushToDB(table_name="passing_types_stats", df=passing_types_stats, conn=conn)

# Creating/Updating the goal_shot_creation_stats table  in the football-db database
pushToDB(table_name="goal_shot_creation_stats", df=goal_shot_creation_stats, conn=conn)

# Creating/Updating the defensive_action_stats table  in the football-db database
pushToDB(table_name="defensive_action_stats", df=defensive_action_stats, conn=conn)

# Creating/Updating the possession_stats table  in the football-db database
pushToDB(table_name="possession_stats", df=possession_stats, conn=conn)

# Creating/Updating the playing_time_stats table  in the football-db database
pushToDB(table_name="playing_time_stats", df=playing_time_stats, conn=conn)

# Creating/Updating the miscellaneous_stats table  in the football-db database
pushToDB(table_name="miscellaneous_stats", df=miscellaneous_stats, conn=conn)

# Creating/Updating the scores_and_fixtures table  in the football-db database
pushToDB(table_name="scores_and_fixtures", df=raw_scores_and_fixtures, conn=conn)

# Close the connection
conn.close()

print("Data Loading Phase Ended....")
