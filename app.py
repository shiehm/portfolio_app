import os
import secrets
from portfolio.database_persistence import DatabasePersistence

from functools import wraps
from flask import (
    flash,
    Flask,
    g,
    redirect,
    render_template,
    request,
    url_for,
)
from werkzeug.exceptions import NotFound

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

@app.before_request
def load_data():
    g.storage = DatabasePersistence()

@app.route('/')
def index():
    return redirect(url_for('get_holdings'))

@app.route('/holdings')
def get_holdings():
    lists = g.storage.all_holdings()
    return render_template('layout.html', lists=lists)

@app.route('/accounts')
def get_accounts():
    lists = g.storage.all_accounts()
    return render_template('layout.html', lists=lists)

@app.route('/assets')
def get_assets():
    lists = g.storage.all_assets()
    return render_template('layout.html', lists=lists)

@app.route('/holdings/<int:account_id>')
def show_holdings_for_account(account_id):
    lists = g.storage.account_holdings(account_id)
    return render_template('layout.html', lists=lists)

@app.route('/accounts/new')
def add_account():
    return render_template('new_account.html')

@app.route('/accounts', methods=['POST'])
def create_account():
    account_name = request.form['account_name'].strip()
    account_type = request.form['account_type']

    g.storage.add_account(account_name, account_type)
    flash("The account has been added.", "success")
    return redirect(url_for('get_accounts'))

@app.route('/assets/new')
def add_asset():
    return render_template('new_asset.html')

@app.route('/assets', methods=['POST'])
def create_asset():
    asset_ticker = request.form['asset_ticker']
    asset_name = request.form['asset_name'].strip()
    asset_category = request.form['asset_category']
    asset_price = request.form['asset_price']

    g.storage.add_asset(asset_ticker, asset_name, asset_category, asset_price)
    flash("The asset has been added.", "success")
    return redirect(url_for('get_assets'))

if __name__ == "__main__":
    if os.environ.get('FLASK_ENV') == 'production':
        app.run(debug=False)
    else:
        app.run(debug=True, port=5003)