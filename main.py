from flask import Flask, render_template
from daily_check import main as daily_check_app
import json


app = Flask(__name__)


@app.route('/')
def daily_market_check():
    result = daily_check_app()
    daily_market_return = result[0]
    daily_market_return = json.loads(daily_market_return)
    base_date = result[1]
    latest_date = result[2]
    print(daily_market_return)

    return render_template('index.html', result=daily_market_return, base_date=base_date, latest_date=latest_date)


if __name__ == '__main__':
    app.run(debug=True)
