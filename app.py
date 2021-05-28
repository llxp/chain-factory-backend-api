from src.app import application, init_app

init_app(application)

if __name__ == '__main__':
    application.debug = True
    #init_app(application)
    application.run(host='0.0.0.0', port=5003)
