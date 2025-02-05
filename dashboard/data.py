import plotly.graph_objects as go
import pandas as pd
from numpy import ones_like
from datetime import datetime, timedelta
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
      self.forecast = pd.read_sql('SELECT * FROM forecast', con=connection)\
                        .groupby('currency')

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

  def get_currency_index(self, curr: str) -> int | None:
    """
    Gets the currency index, needed to access the data. If it doesn't exist - returns None.
    """

    try:
      mask = self.currency_names['name'] == curr
      ent = self.currency_names[mask].index[0]

      return ent

    except KeyError:
      return
  
  def get_ex_rate_graph(self, curr_1: str, curr_2: str, timeframe: int):
    """
    Creates a graph of the exchange rates, given two currencies and a time frame(integer in days).

    :param curr_1: First currency
    :param curr_2: Second currency
    :param timeframe: Time frame in days
    """

    if curr_1 == curr_2:
      return

    # Euro is a special case: there are no cases of euro in pair dataframe
    if curr_1 == 'EUR':
      return self.plot_euro_ex(curr_2, timeframe)
    if curr_2 == 'EUR':
      return self.plot_euro_ex(curr_1, timeframe)
    
    ent_1 = self.get_currency_index(curr_1)
    ent_2 = self.get_currency_index(curr_2)

    # Handle the case, when one of the currencies is not available in the dataset
    if ent_1 is None or ent_2 is None:
      return

    group = (ent_1, ent_2) if ent_1 > ent_2 else (ent_2, ent_1)
    name_1, name_2 = (curr_1, curr_2) if ent_1 > ent_2 else (curr_2, curr_1)
    data = self.pair_df.get_group(group)

    # Update dag is not yet finished, so I had to artificially add 7 days
    earliest = datetime.today() - timedelta(days=timeframe+7) 
    mask = (data['date'] >= earliest if timeframe != -1 else
            ones_like(data['date'].values, dtype=bool))

    return px.line(
      data[mask].sort_values('date'),
      x = 'date',
      y = 'rate',
      labels={
        'date': 'Date',
        'rate': 'Exchange rate'
      },
      title=f'Exchange rate of {name_1}/{name_2}'
    )

  def plot_euro_ex(self, curr: str, timeframe: int):
    ent = self.get_currency_index(curr)

    if not ent:
      return go.Figure()\
        .update_layout(showlegend=False,
                       plot_bgcolor='rgba(0, 0, 0, 0)')\
        .add_annotation(x=2, y=2, text='No data available',
                        font={"size": 50, "color": "red"},
                        showarrow=False)\
        .update_xaxes(visible=False)\
        .update_yaxes(visible=False)

    data = self.ex_rates.get_group(ent)
    earliest = datetime.today() - timedelta(days=timeframe+7)

    # -1 in this context is a special value meaning: maximum days
    mask = (data['time_period'] >= earliest if timeframe != -1 else
            ones_like(data['time_period'].values, dtype=bool))

    return px.line(
      data[mask].sort_values('time_period'),
      x = 'time_period',
      y = 'rate',
      labels={
        'time_period': 'Date',
        'rate': 'Exchange rate'
      },
      title=f'Exchange rate of {curr}/EUR'
    )

  def plot_forecast(self, curr_1, curr_2):
    if curr_1 == "EUR":
      return self.plot_forecast_euro(curr_2)
    if curr_2 == "EUR":
      return self.plot_forecast_euro(curr_1)

    if curr_1 == curr_2:
      return
    
    forecast_1 = self.get_forecast(curr_1)
    forecast_2 = self.get_forecast(curr_2)

    if forecast_1 is None or forecast_2 is None:
      return 

    forecast_1, forecast_2 = forecast_1.set_index(['index']), forecast_2.set_index(['index'])

    forecast_1['fore1'] = forecast_1.loc[:, '0']
    forecast_1['fore2'] = forecast_2.loc[:, '0']
    forecast_1['fore'] = forecast_1['fore1'] / forecast_1['fore2']

    return px.line(
      forecast_1.reset_index(),
      x='index',
      y='fore',
      labels={
        'index': 'Date',
        'fore': 'Forecast'
      },
      title='Forecast'
    )
  
  def plot_forecast_euro(self, curr: str):
    forecast = self.get_forecast(curr)

    if forecast is None:
      return go.Figure()\
        .update_layout(showlegend=False,
                       plot_bgcolor='rgba(0, 0, 0, 0)')\
        .add_annotation(x=2, y=2, text='No forecast available',
                        font={"size": 50, "color": "red"},
                        showarrow=False)\
        .update_xaxes(visible=False)\
        .update_yaxes(visible=False)

    return px.line(
      forecast,
      x='index',
      y='0',
      labels={
        'index': 'Date',
        '0': 'Forecast'
      },
      title='Forecast'
    )

  def get_forecast(self, curr) -> pd.DataFrame | None:
    try:
      ent = self.get_currency_index(curr)
      data = self.forecast.get_group(ent)

      mask = data['forecast_error'].isna()
      return data[mask]

    except KeyError:
      return

if __name__ == "__main__":
  plotter = DataPlotter("postgresql://airflow:airflow@localhost:5454/forex")
  fig = plotter.plot_forecast('BRL', 'EUR')
  fig.show()