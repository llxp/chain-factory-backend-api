from flask import Flask
import os
import sys
from flask_cors import CORS

from .api import app as api
from .login_api.login_api import \
    app as login_api, \
    init_app as ial


print('initializing', file=sys.stderr)

# create the flask application object
application = Flask(__name__)
application.config['SECRET_KEY'] = os.getenv('APP_SECRET', 'SECRET')
mongodb_connection: str = os.getenv('MONGODB_CONNECTION_URI', 'mongodb://root:example@mongodb/orchestrator_db?authSource=admin')
mongodb_db: str = os.getenv('MONGODB_DB', 'orchestrator_db')
application.config['MONGODB_SETTINGS'] = {
    'host': mongodb_connection,
    'db': mongodb_db
}
application.config['MONGOALCHEMY_CONNECTION_STRING'] = mongodb_connection
application.config['MONGOALCHEMY_DATABASE'] = mongodb_db

cors = CORS(application)

application.register_blueprint(api, url_prefix='/api/orchestrator/')
application.register_blueprint(login_api, url_prefix='/api/login/')


def init_app(app):
    ial(app)


@application.route(
    '/api/',
    methods=['GET'])
def index(service: str, port: str, action: str):
    pass


if __name__ == '__main__':
    application.debug = True
    init_app(application)
    application.run(host='0.0.0.0', port=5003)
