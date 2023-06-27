from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import os

import numpy as np
from scipy.stats import rankdata

# user = "user"
# password = "password"
# host = "192.168.59.101"
# port = "30432"
# database = "football-db"

# Database Configurations
database = os.environ["database"]
user = os.environ["user"]
password = os.environ["password"]
host = os.environ["host"]
port = os.environ["port"]


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


def inverse_percentile_rank(data, min_percentile=35, max_percentile=100):
    """
    Ranks a data in a pandas series in an inverse manner
    between the min and max percentile values.

    Args:
        data (pd.Series): Data Series that needs to be inversly ranked.
        min_percentile (int, optional): Minimum rank. Defaults to 35.
        max_percentile (int, optional): Maximum rank. Defaults to 100.

    Returns:
        pd.Series: Inverse Ranked Series.
    """
    ranks = rankdata(data)
    min_rank = np.min(ranks)
    max_rank = np.max(ranks)
    scaled_ranks = (
        (max_percentile - min_percentile)
        * (1 - (ranks - min_rank) / (max_rank - min_rank))
    ) + min_percentile
    return scaled_ranks


def percentile_rank(data, min_percentile=40, max_percentile=100):
    """
    Ranks a data in a pandas series between
    the min and max percentile values.

    Args:
        data (pd.Series): Data Series that needs to be ranked.
        min_percentile (int, optional): Minimum rank. Defaults to 35.
        max_percentile (int, optional): Maximum rank. Defaults to 100.

    Returns:
        pd.Series: Ranked Series.
    """
    ranks = rankdata(data)
    min_rank = np.min(ranks)
    max_rank = np.max(ranks)
    scaled_ranks = ((ranks - min_rank) / (max_rank - min_rank)) * (
        max_percentile - min_percentile
    ) + min_percentile
    return scaled_ranks


def get_attack_data(conn):
    """
    Get the attack data, rank the data and calculate mean ranking.

    Args:
        conn (sqlalchemy.engine.base.Engine.Connection): Connection to the postgres DB.

    Returns:
        pd.DataFrame: attack ranking data.
    """
    attack = pd.read_sql_query(
        sql="""SELECT t1.squad, standard_gls, standard_sot, poss FROM shooting_stats as t1 
                    INNER JOIN standard_stats as t2 ON t1.squad = t2.squad 
                                                        AND t1.value = t2.value where t1.value = 'squad' """,
        con=conn,
    )

    attack[["standard_gls", "standard_sot", "poss"]] = attack[
        ["standard_gls", "standard_sot", "poss"]
    ].apply(percentile_rank)

    attack.loc[:, "attack"] = attack[["standard_gls", "standard_sot", "poss"]].mean(
        axis=1
    )

    return attack


def get_defence_data(conn):
    """
    Get the defence data, rank the data and calculate mean ranking.

    Args:
        conn (sqlalchemy.engine.base.Engine.Connection): Connection to the postgres DB.

    Returns:
        pd.DataFrame: defence ranking data.
    """
    defence = pd.read_sql_query(
        sql="""SELECT t1.squad, int, tackles_tkl, tackles_tklw, performance_ga, performance_sota FROM defensive_action_stats as t1 
                  INNER JOIN goalkeeping_stats as t2 ON t1.squad = t2.squad 
                                                    AND t1.value = t2.value where t1.value = 'squad' """,
        con=conn,
    )

    defence.loc[:, "tackles_win_pct"] = (
        defence.loc[:, "tackles_tklw"].astype("float16")
        / defence.loc[:, "tackles_tkl"].astype("float16")
    ) * 100

    defence[["tackles_win_pct", "int"]] = defence[["tackles_win_pct", "int"]].apply(
        percentile_rank
    )
    defence[["performance_ga", "performance_sota"]] = defence[
        ["performance_ga", "performance_sota"]
    ].apply(inverse_percentile_rank)

    defence.loc[:, "defence"] = defence[
        ["tackles_win_pct", "int", "performance_ga", "performance_sota"]
    ].mean(axis=1)

    return defence


def get_midfield_data(conn):
    """
    Get the midfield data, rank the data and calculate mean ranking.

    Args:
        conn (sqlalchemy.engine.base.Engine.Connection): Connection to the postgres DB.

    Returns:
        pd.DataFrame: midfield ranking data.
    """

    midfield = pd.read_sql_query(
        sql="""SELECT t1.squad, passtypes_crs, outcomes_off, outcomes_blocks, ast, kp FROM passing_types_stats as t1 
                  INNER JOIN passing_stats as t2 ON t1.squad = t2.squad 
                                                    AND t1.value = t2.value where t1.value = 'squad' """,
        con=conn,
    )

    midfield[["passtypes_crs", "ast", "kp"]] = midfield[
        ["passtypes_crs", "ast", "kp"]
    ].apply(percentile_rank)
    midfield[["outcomes_off", "outcomes_blocks"]] = midfield[
        ["outcomes_off", "outcomes_blocks"]
    ].apply(inverse_percentile_rank)

    midfield.loc[:, "midfield"] = midfield[
        ["outcomes_off", "outcomes_blocks", "passtypes_crs", "ast", "kp"]
    ].mean(axis=1)
    return midfield


def merge_data(attack, midfield, defence):
    """
    Merges three data frames into one base on a
    common column called "squad".


    Args:
        attack (pd.DataFrame): Dataframe containing the attack ratings.
        midfield (pd.DataFrame): Dataframe containing the midfield ratings.
        defence (pd.DataFrame): Dataframe containing the defence ratings.

    Returns:
        pd.DataFrame: Dataframe containing all three attack, midfield and defence ratings.
    """
    try:
        return pd.merge(
            attack[["squad", "attack"]], midfield[["squad", "midfield"]], on="squad"
        ).merge(defence[["squad", "defence"]], on="squad")
    except Exception as e:
        print("Unable to merge Attack, Midfield and Defence dataframes....")
        raise (e)


if __name__ == "__main__":
    # Connect to DB
    try:
        print("Establishing Connection with DB....")
        db = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        conn = db.connect()
        print("Successfully Established Connection with DB....")

    except Exception as e:
        print("Unable to Establish Connection with DB....")
        raise (e)

    # Get Attack, Midfield and Defence data
    attack = get_attack_data(conn=conn)
    print("Getting Attack Data and Calculating Ratings...")
    midfield = get_midfield_data(conn=conn)
    print("Getting Midfield Data and Calculating Ratings...")
    defence = get_defence_data(conn=conn)
    print("Getting Defence Data and Calculating Ratings...")

    # Merging the data
    data = merge_data(attack=attack, midfield=midfield, defence=defence)

    # Calculating the overall rating
    data.loc[:, "overall"] = data[["attack", "midfield", "defence"]].mean(axis=1)

    # Pushing to DB
    pushToDB(table_name="ratings", df=data, conn=conn)

    # Close the connection
    conn.close()

    print("Rating Successfully Loaded....")
