# Flask app to publish workbook to NocoDB

from flask import Flask, request, jsonify
import os
import sys
import yaml
import requests
import logging
import dotenv


from flask_oidc import OpenIDConnect
from flask import g, session
from werkzeug.middleware.proxy_fix import ProxyFix


from authlib.integrations.flask_oauth2 import current_token


# configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # logging.FileHandler("nocodb_export.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Load environment variables from .env file
dotenv.load_dotenv()


app = Flask(__name__)
app.config.from_prefixed_env()
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1, x_port=1)

oidc = OpenIDConnect(app)


@app.route('/callback', methods=['GET', 'POST'])
def callback():
    """
    Handle the callback from the OpenID Connect provider.
    """
    print("Callback received")
    return "Callback received"

# /workbook/:lang/:workbook/:exercise`
@app.route('/workbook/<lang>/<workbook>/<exercise>', methods=['GET'])
@oidc.require_login
def publish_workbook(lang, workbook, exercise):
    """
    Publish a workbook to NocoDB.
    """
    # Load the YAML file
    profile = session["oidc_auth_profile"]
    print(f"Profile: {profile}")
    html = f"<html><body><h1>Publish workbook</h1><h2>lang: {lang}</h2><h2>workbook: {workbook}</h2><h2>exercise: {exercise}</h2>Profile: { profile }</body></html>"
    return html

app.run(host='0.0.0.0', port=5000)
