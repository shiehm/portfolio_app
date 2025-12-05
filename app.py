import json
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
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.secret_key = secrets.token_hex(32)

@app.before_request
def load_data():
    g.storage = DatabasePersistence()

@app.route('/')
def index():
    return render_template('layout.html')

@app.route('/holdings')
def get_holdings():
    columns = g.storage.get_columns()
    accounts = g.storage.all_accounts()
    account_id = request.args.get('account_id', type=int)
    
    if account_id is None:
        lists = g.storage.all_holdings()
    else:
        lists = g.storage.account_holdings(account_id)
        # if holdings:
        #     lists = holdings
        # else:
        #     lists = [{}]
    
    return render_template('holdings.html',
                            columns=columns, 
                            accounts=accounts, 
                            account_id=account_id,
                            lists=lists)

@app.route('/accounts')
def get_accounts():
    pass
    # lists = g.storage.all_accounts()
    # return render_template('accounts.html', lists=lists)

@app.route('/assets')
def get_assets():
    pass
    # lists = g.storage.all_assets()
    # return render_template('assets.html', lists=lists)

# @app.route('/holdings/<int:account_id>')
# def show_holdings_for_account(account_id):
#     lists = g.storage.account_holdings(account_id)
#     return render_template('holdings.html', lists=lists)

@app.route('/accounts/new')
def add_account():
    return render_template('new_account.html')

@app.route('/accounts', methods=['POST'])
def create_account():
    account_name = request.form['account_name'].strip()
    account_type = request.form['account_type']

    g.storage.add_account(account_name, account_type)
    flash("The account has been added.", "success")
    return redirect(url_for('get_holdings'))

@app.route('/assets/new')
def add_asset():
    accounts = g.storage.all_accounts()
    return render_template('new_asset.html', accounts = accounts)

@app.route('/assets', methods=['POST'])
def create_asset():
    print("FORM DATA:", request.form.to_dict(flat=False))
    asset_ticker = request.form['asset_ticker']
    asset_name = request.form['asset_name'].strip()
    asset_category = request.form['asset_category']
    current_price = request.form.get('current_price', 0)

    g.storage.add_asset(asset_ticker, asset_name, asset_category, current_price)
    flash("The asset has been added.", "success")
    return redirect(url_for('get_holdings'))

@app.route('/holdings/new')
def add_holding():
    accounts = g.storage.all_accounts()
    assets = g.storage.all_assets()
    return render_template('new_holding.html', accounts=accounts, assets=assets)

@app.route('/holdings', methods=['POST'])
def create_holding():
    account_id = json.loads(request.form['account_id'])
    asset_id = json.loads(request.form['asset_id'])
    shares = request.form['shares']
    
    g.storage.add_holding(account_id, asset_id, shares)
    flash("The holding has been added.", "success")
    return redirect(url_for('get_holdings'))

@app.route("/holdings/delete", methods=["POST"])
def delete_holding():
    holding_id = request.form.get('holding_id', 0)
    g.storage.delete_holding(holding_id)
    flash("The holding has been deleted.", "success")
    return redirect(url_for('get_holdings'))

@app.route("/holdings/update", methods=["GET", "POST"])
def update_holding():
    if request.method == "POST":
        holding_id = request.form.get('holding_id', type=int)
        shares = request.form.get('shares', type=int)
        g.storage.update_holding(holding_id, shares)
        flash("The holding has been updated.", "success")
        return redirect(url_for('get_holdings'))
    else:
        holding_id = request.args.get('holding_id', type=int)

        print("ARGS RAW:", request.args)              # see raw strings
        print("holding_id raw:", request.args.get("holding_id"))


        holding = g.storage.find_holding(holding_id)
        return render_template('update_holding.html', holding=holding)

@app.route("/assets/delete", methods=["POST"])
def delete_asset():
    asset_id = request.form.get('asset_id', 0)
    g.storage.delete_asset(asset_id)
    flash("The asset has been deleted.", "success")
    return redirect(url_for('get_holdings'))

@app.route("/accounts/delete", methods=["POST"])
def delete_account():
    account_id = request.form.get('account_id', 0)
    g.storage.delete_account(account_id)
    flash("The account has been deleted.", "success")
    return redirect(url_for('get_holdings'))

if __name__ == "__main__":
    if os.environ.get('FLASK_ENV') == 'production':
        app.run(debug=False)

    else:
        app.run(debug=True, port=5003)