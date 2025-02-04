import pandas as pd
import plotly.express as px

from sqlalchemy import create_engine

class DataPlotter:
  def __init__(self, db_uri: str):
    # Create engine
    engine = create_engine(db_uri)

    with engine.connect() as con:
      # Get a connection
      connection = con.connection

      # Import data tables
      self.pair_df = pd.read_sql_query('SELECT * FROM ex_pairs', con=connection)\
                       .groupby(['currency_1', 'currency_2'])
      self.macro = pd.read_sql('SELECT * FROM macro', con=connection)

      # Import the dimension tables
      self.bop_measure = pd.read_sql_query('SELECT * FROM bop_measure_final', con=connection)\
                           .set_index('index')
      self.inr_measure = pd.read_sql_query('SELECT * FROM inr_measure_final', con=connection)\
                           .set_index('index')
      self.entities = pd.read_sql_query('SELECT * FROM entity_dimension_final', con=connection)\
                           .set_index('index')
      self.currency_names = pd.read_sql_query('SELECT * FROM dim_currency', con=connection)\
                            .set_index('ex_id')


  def get_currency_options(self) -> list[str]:
    pairs = [(int(ent_1), int(ent_2)) 
             for (ent_1, ent_2), _ in self.pair_df]
    
    names = [f"{self.entities.loc[ent_1, 'currency_code']}/{self.entities.loc[ent_2, 'currency_code']}"
             for ent_1, ent_2 in pairs]

    return names
  
  def get_ex_rate_graph(self, pair_str: str):
    curr_1, curr_2 = tuple(pair_str.split("/"))
    
    mask_1 = self.currency_names['name'] == curr_1
    mask_2 = self.currency_names['name'] == curr_2

    ent_1 = self.currency_names[mask_1].index[0]
    ent_2 = self.currency_names[mask_2].index[0]

    group = (ent_1, ent_2) if ent_1 > ent_2 else (ent_2, ent_1)

    data = self.pair_df.get_group(group)

    return px.line(
      data,
      x = 'date',
      y = 'rate'
    )

if __name__ == "__main__":
  plotter = DataPlotter("postgresql://airflow:airflow@localhost:5454/forex")
  fig = (plotter.get_ex_rate_graph('JPY/USD'))
  fig.show()