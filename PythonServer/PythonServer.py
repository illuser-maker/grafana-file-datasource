#!/usr/bin/python

"""Скрипт запуска сервера. Обрабатывает GET-запрос на свой /folder/ и
POST запросы на адреса /folder/sources, /folder/search, /folder/query.
Сервер обрабатывает json запросы с сервиса grafana-server.
Для обработки запросов служит класс QueryHandler, а для обработки
файлов - FileHandler."""

from flask import Flask, request, jsonify, json, abort
from flask_cors import CORS, cross_origin

import pandas as pd
import numpy as np
import getopt
import os
import sys

from datetime import datetime
from QueryHandler import QueryHandler

APP = Flask(__name__)
APP_CORS = CORS(APP)
APP.config['CORS_HEADERS'] = 'Content-Type'
METHODS = ('GET', 'POST')
PATH = './'
QH = None

# ----------------------------------------------------------------------------------
@APP.route('/<folder>/', methods=METHODS)
@APP.route('/<folder>', methods=METHODS)
@cross_origin()
def hello_world(folder):
    """Функция проверяет, есть ли такая папка, и возвращает ОК в случае существования"""
    if os.path.isdir(PATH + str(folder)):
        return 'CSV Python Grafana datasource for '+str(folder)
    else:
        return abort(404)

# ----------------------------------------------------------------------------------
@APP.route('/<folder>/sources', methods=METHODS)
@cross_origin(max_age=600)
def query_routes(folder):
    """Функция возвращает список файлов формата csv в папке. Используется для поля Source"""
    d_type = request.get_json().get('type', 'timeseries')
    return jsonify(QH.get_sources(d_type=d_type, folder=str(folder)))

# ----------------------------------------------------------------------------------
@APP.route('/<folder>/search', methods=METHODS)
@cross_origin()
def find_metrics(folder):
    """Функция возвращает список метрик, которые надо запросить. Используется для поля Metric.
    В текущем варианте просто возвращает список столбцов."""
    req = request.get_json()
    QH.get_sources('table', folder)
    source = req.get('source', '')
    if source == 'select source':
        return jsonify([])
    d_type = req.get('type', 'timeseries')
    target = req.get('target', '')
    return jsonify(QH.get_metrics(d_type, source, target))


def annotations_to_response(target, df):
    response = []

    # Single series with DatetimeIndex and values as text
    if isinstance(df, pd.Series):
        for timestamp, value in df.iteritems():
            response.append({
                "annotation": target, # The original annotation sent from Grafana.
                "time": timestamp.value // 10 ** 6, # Time since UNIX Epoch in milliseconds. (required)
                "title": value, # The title for the annotation tooltip. (required)
                #"tags": tags, # Tags for the annotation. (optional)
                #"text": text # Text for the annotation. (optional)
            })
    # Dataframe with annotation text/tags for each entry
    elif isinstance(df, pd.DataFrame):
        for timestamp, row in df.iterrows():
            annotation = {
                "annotation": target,  # The original annotation sent from Grafana.
                "time": timestamp.value // 10 ** 6,  # Time since UNIX Epoch in milliseconds. (required)
                "title": row.get('title', ''),  # The title for the annotation tooltip. (required)
            }

            if 'text' in row:
                annotation['text'] = str(row.get('text'))
            if 'tags' in row:
                annotation['tags'] = str(row.get('tags'))

            response.append(annotation)
    else:
        abort(404, Exception('Received object is not a dataframe or series.'))

    return response


def _series_to_annotations(df, target):
    if df.empty:
        return {'target': '%s' % (target),
                'datapoints': []}

    sorted_df = df.dropna().sort_index()
    timestamps = (sorted_df.index.astype(pd.np.int64) // 10 ** 6).values.tolist()
    values = sorted_df.values.tolist()

    return {'target': '%s' % (df.name),
            'datapoints': zip(values, timestamps)}

# ----------------------------------------------------------------------------------
@APP.route('/<folder>/query', methods=METHODS)
@cross_origin(max_age=600)
def query_metrics(folder):
    """Обработчик запроса данных. Должен вернуть данные."""
    req = request.get_json()
    QH.get_sources('table', folder)

    #ts_range = {'$gt': pd.Timestamp(req['range']['from']).to_pydatetime(),
    #            '$lte': pd.Timestamp(req['range']['to']).to_pydatetime()}
    #ts = pd.date_range(ts_range['$gt'], ts_range['$lte'], freq='H')
    #if 'intervalMs' in req:
    #    freq = str(req.get('intervalMs')) + 'ms'
    #else:
    #    freq = None

    results = QH.get_data(req)
    return jsonify(results)

#----------------------------------------------------------------------------------
@APP.route('/<folder>/annotations', methods=METHODS)
@cross_origin(max_age=600)
def query_annotations(folder):
    print (request.headers, request.get_json())
    req = request.get_json()

    results = []

    ts_range = {'$gt': pd.Timestamp(req['range']['from']).to_pydatetime(),
                '$lte': pd.Timestamp(req['range']['to']).to_pydatetime()}

    query = req['annotation']['query']

    if ':' not in query:
        abort(404, Exception('Target must be of type: <finder>:<metric_query>, got instead: ' + query))

    finder, target = query.split(':', 1)
    results.extend(annotations_to_response(query, annotation_readers[finder](target, ts_range)))

    return jsonify(results)

# ----------------------------------------------------------------------------------
def main(argv):
    global PATH
    port = 3003
    debug = False
    addr = '0.0.0.0'
    try:
        opts, args = getopt.getopt(argv,"hvp:f:a:",["port=","folder=","addr="])
    except getopt.GetoptError:
        print ('PythonServer.py -p <port> -f <folder>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('PythonServer.py -p <port> -f <folder>')
            sys.exit()
        elif opt in ("-a", "--addr"):
            addr = arg
        elif opt in ("-p", "--port"):
            port = int(arg)
        elif opt in ("-f", "--folder"):
            PATH = arg
        elif opt in ("-v"):
            debug = True
    if PATH[-1] != '/':
        PATH += '/'
    global QH
    QH = QueryHandler(PATH, filetypes=['csv'])
    APP.run(host=addr, port=port, debug=debug)


if __name__ == '__main__':
    main(sys.argv[1:])
