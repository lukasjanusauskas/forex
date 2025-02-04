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
      self.ex_rates = pd.read_sql_query('SELECT * FROM ex_rates', con=connection)\
                        .groupby('currency')
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
    # Get the list of all currencies available in pair_df
    labels = ["EUR"]
    labels.extend(list(self.currency_names.name))

    return labels
  
  def get_ex_rate_graph(self, curr_1: str, curr_2: str):
    # If both currencies are the same: do nothing and return
    if curr_1 == curr_2:
      return

    # Euro is a special case: there are no cases of euro in pair dataframe
    if curr_1 == 'EUR':
      return self.plot_euro_ex(curr_2, True)
    if curr_2 == 'EUR':
      return self.plot_euro_ex(curr_1, False)
    
    mask_1 = self.currency_names['name'] == curr_1
    mask_2 = self.currency_names['name'] == curr_2

    try: 
      ent_1 = self.currency_names[mask_1].index[0]
      ent_2 = self.currency_names[mask_2].index[0]
    except KeyError:
      print("One of the currencies was not found")
      return

    group = (ent_1, ent_2) if ent_1 > ent_2 else (ent_2, ent_1)
    data = self.pair_df.get_group(group)

    return px.line(
      data,
      x = 'date',
      y = 'rate',
      labels={
        'date': 'Date',
        'rate': 'Exchange rate'
      },
      title=f'Exchange rate of {curr_2}/{curr_1}'
    )

  def plot_euro_ex(self, curr: str, first: bool):
    mask = self.currency_names['name'] == curr
    ent = self.currency_names[mask].index[0]

    data = self.ex_rates.get_group(ent)

    return px.line(
      data,
      x = 'time_period',
      y = 'rate',
      labels={
        'date': 'Date',
        'rate': 'Exchange rate'
      },
      title=f'Exchange rate of {curr}/EUR'
    )

if __name__ == "__main__":
  plotter = DataPlotter("postgresql://airflow:airflow@localhost:5454/forex")
  fig = (plotter.get_ex_rate_graph('JPY/USD'))
  fig.show()