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
      # Import the master data table(the fact table)
      self.df = pd.read_sql_query('SELECT * FROM master', con=connection)

      # Import the dimension tables
      self.bop_measure = pd.read_sql_query('SELECT * FROM bop_measure_final', con=connection)\
                           .set_index('index')
      self.inr_measure = pd.read_sql_query('SELECT * FROM inr_measure_final', con=connection)\
                           .set_index('index')
      self.entities = pd.read_sql_query('SELECT * FROM entity_dimension_final', con=connection)\
                           .set_index('index')

    self.pair_df = self._get_pairs()

  def get_currency_options(self) -> list[str]:
    pairs = [(int(ent_1), int(ent_2)) 
             for (ent_1, ent_2), _ in self.pair_df]
    
    names = [f"{self.entities.loc[ent_1, 'currency_code']}/{self.entities.loc[ent_2, 'currency_code']}"
             for ent_1, ent_2 in pairs]

    return names
  
  def get_ex_rate_graph(self, pair_str: str):
    curr_1, curr_2 = tuple(pair_str.split("/"))
    
    mask_1 = self.entities['currency_code'] == curr_1
    mask_2 = self.entities['currency_code'] == curr_2

    ent_1 = self.entities[mask_1].index[0]
    ent_2 = self.entities[mask_2].index[0]

    data = self.pair_df.get_group((ent_1, ent_2))

    return px.line(
      data,
      x = 'date',
      y = 'ex_rate'
    )

  def _get_pairs(self) -> pd.DataFrame:
    # Join the dataframe to itself to get pairs of entites
    pair_df = pd.merge(self.df, self.df, on=['bop_measure', 'inr_measure', 'date'])

    # Make sure, that there are no repeating pairs
    mask = pair_df['entity_x'] < pair_df['entity_y']
    pair_df = pair_df[mask]
    
    # Get the exchange rate
    pair_df['ex_rate'] = pair_df['ex_rate_x'] / pair_df['ex_rate_y']
    pair_df = pair_df[['entity_x', 'entity_y', 'date', 'ex_rate']]
    pair_df.drop_duplicates(inplace=True)
    pair_df = pair_df.sort_values('date')

    return pair_df.groupby(['entity_x', 'entity_y'])
  

if __name__ == "__main__":
  plotter = DataPlotter("postgresql://airflow:airflow@localhost:5454/forex")
  fig = (plotter.get_ex_rate_graph('JPY/USD'))
  fig.show()