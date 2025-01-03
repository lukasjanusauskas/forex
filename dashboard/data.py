import pandas as pd

from sqlalchemy import create_engine


def import_data() -> list[pd.DataFrame]:
  # Create engine
  engine = create_engine("postgresql://airflow:airflow@localhost:5454/forex")

  with engine.connect() as con:
    # Get a connection
    connection = con.connection
    # Import the master data table(the fact table)
    df = pd.read_sql_query('SELECT * FROM master', con=connection)

    # Import the dimension tables
    bop_measure = pd.read_sql_query('SELECT * FROM bop_measure_final', con=connection)
    inr_measure = pd.read_sql_query('SELECT * FROM inr_measure_final', con=connection)
    entities = pd.read_sql_query('SELECT * FROM entity_dimension_final', con=connection)

    return df, bop_measure, inr_measure, entities


def get_pairs(df: pd.DataFrame) -> pd.DataFrame:
  # Join the dataframe to itself to get pairs of entites
  pair_df = pd.merge(df, df, on=['bop_measure', 'inr_measure', 'date'])

  # Make sure, that there are no repeating pairs
  mask = pair_df['entity_x'] < pair_df['entity_y']
  pair_df = pair_df[mask]
  
  # Get the exchange rate
  pair_df['ex_rate'] = pair_df['ex_rate_x'] / pair_df['ex_rate_y']

  return pair_df[['entity_x', 'entity_y', 'date', 'ex_rate']]