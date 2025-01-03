from dash import Dash, html
from data import import_data, get_pairs


app = Dash(
  external_scripts=[{"src": "https://cdn.tailwindcss.com"}]
)

app.layout = [
  html.Div(children='FOREX and related indicator dashboard',
           className='text-3xl font-bold')
]

if __name__ == '__main__':
  app.run(debug=True)