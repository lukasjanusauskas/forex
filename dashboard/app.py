from dash import Dash, html, dcc, callback, Input, Output
from data import DataPlotter

from datetime import timedelta


app = Dash(
  external_scripts=[{"src": "https://cdn.tailwindcss.com"}]
)
plotter = DataPlotter("postgresql://airflow:airflow@localhost:5454/forex")

currency_options = plotter.get_currency_options()

app.layout = [
  html.Div(className='bg-slate-900 text-slate-200 min-w-screen min-h-screen pl-5 pt-5',
    children= [
     html.H1('FOREX dashboard',
              className='text-3xl font-bold'),

      html.Div(className="flex flex-row gap-5 my-2",
      children = [
       html.P('Currencies:',
              className='text-xl font-bold'),

       dcc.Dropdown(
         id='dropdown-currency1',
         className='w-32',
         options=currency_options,
         value='USD'
       ),

       dcc.Dropdown(
         id='dropdown-currency2',
         className='w-32',
         options=currency_options,
         value='GBP'
       ),

        html.P('Time frame:',
               className='text-xl font-bold'),

        dcc.RadioItems(
          options=[
            {'label': html.Span('1W', className='ml-2 mr-4'), 'value': 7},
            {'label': html.Span('1M', className='ml-2 mr-4'), 'value': 30},
            {'label': html.Span('1Y', className='ml-2 mr-4'), 'value': 365},
            {'label': html.Span('5Y', className='ml-2 mr-4'), 'value': 365 * 5},
            {'label': html.Span('MAX', className='ml-2 mr-4'), 'value': -1}
          ],
          id='time_frame_buttons',
          value=7,
          inline=True
        )
      ]
     ),

     html.Div(className="flex flex-row gap-20",
        children= [dcc.Graph(id="ex_rate_graph", className="w-2/5"),
                   dcc.Graph(id="forecast_graph", className="w-2/5")]
     )
    ])
]

# Callbacks for interactivity
@callback(
  Output('ex_rate_graph', 'figure'),
  Input('dropdown-currency1', 'value'),
  Input('dropdown-currency2', 'value'),
  Input('time_frame_buttons', 'value')
)
def update_ex_rate(curr_1: str, curr_2: str, 
                   timeframe: int):

  return plotter.get_ex_rate_graph(curr_2, curr_1, timeframe)

@callback(
  Output('forecast_graph', 'figure'),
  Input('dropdown-currency1', 'value'),
  Input('dropdown-currency2', 'value')
)
def update_forecast(curr_1: str, curr_2: str):
  return plotter.plot_forecast(curr_2, curr_1)

if __name__ == '__main__':
  app.run(debug=True)