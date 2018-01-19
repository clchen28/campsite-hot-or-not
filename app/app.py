import uuid
import configparser
from flask import Flask
from flask_restful import Resource, Api
from flask_cassandra import CassandraCluster

app = Flask(__name__)
cassandra = CassandraCluster()
config = configparser.ConfigParser()
config.read("flask.cfg")
config.get("cassandra_cluster", "host")
app.config['CASSANDRA_NODES'] = [config.get("cassandra_cluster", "host")]

@app.route("/cassandra_test")
def cassandra_test():
    session = cassandra.connect()
    session.set_keyspace("weather_stations")
    cql = "SELECT * FROM readings LIMIT 10"
    r = session.execute(cql)
    return str(r[0])

if __name__ == '__main__':
    app.run()