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
     dcc.RadioItems(
       id='radio',
       className='flex flex-row gap-5 mt-10',
       options=currency_options,
       value=currency_options[0],
       inline=True
     ),
     dcc.Graph(id="ex_rate_graph")
    ])
]

# Callbacks for interactivity
@callback(
  Output('ex_rate_graph', 'figure'),
  Input('radio', 'value')
)
def update_ex_rate(pair_str: str):
  return plotter.get_ex_rate_graph(pair_str)

if __name__ == '__main__':
  app.run(debug=True)