from dash import Dash, html, dcc, callback, Input, Output
from data import DataPlotter


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

      html.Div(className="flex flex-row w-1/3 gap-5",
      children = [
       html.P('Pick the currencies',
              className='text-xl font-bold'),

       dcc.Dropdown(
         id='dropdown-currency1',
         className='w-32',
         options=currency_options,
         value=currency_options[0],
       ),

       dcc.Dropdown(
         id='dropdown-currency2',
         className='w-32',
         options=currency_options,
         value=currency_options[1],
       )
      ]
     ),

     dcc.Graph(id="ex_rate_graph",
               className="w-1/2")
    ])
]

# Callbacks for interactivity
@callback(
  Output('ex_rate_graph', 'figure'),
  Input('dropdown-currency1', 'value'),
  Input('dropdown-currency2', 'value')
)
def update_ex_rate(curr_1: str, curr_2: str):
  return plotter.get_ex_rate_graph(curr_2, curr_1)

if __name__ == '__main__':
  app.run(debug=True)