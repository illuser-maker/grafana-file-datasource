from abc import ABC, abstractmethod
import os

from FileHandler import CSVHandler, parse_types
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score
from collections import defaultdict


class QueryHandlerInterface(ABC):
    """Интерфейс обработчика запросов"""

    @abstractmethod
    def get_sources(self, d_type, folder):
        """Функция должна возвращать список файлов, которые можно прочитать."""
        pass

    @abstractmethod
    def get_metrics(self, d_type, source, target=''):
        """Функция должна возвращать список метрик, котороые возможно вернуть."""
        pass

    @abstractmethod
    def get_data(self, request):
        """Функция должна возвращать данные в необходиомом формате."""
        pass


def dataframe_to_response(target, df):
    """Возвращает список временных рядов. Временной ряд:
    {
    "target":"pps in",
    "datapoints":[
      [622,1450754160000],
      [365,1450754220000]
    ]
  }"""
    response = []
    if df.empty:
        return response

    # if freq is not None:
    #    orig_tz = df.index.tz
    #    df = df.tz_convert('UTC').resample(rule=freq, label='right', closed='right', how='mean').tz_convert(orig_tz)

    if isinstance(df, pd.Series):
        response.append(_series_to_response(df))
    elif isinstance(df, pd.DataFrame):
        for col in df.columns:
            response.append(_series_to_response(df[col]))
    else:
        abort(404, Exception('Received object is not a dataframe or series.'))
    return response


def _series_to_response(df):
    sorted_df = df.dropna().sort_index()
    timestamps = sorted_df.index
    values = sorted_df.values.tolist()
    return {'target': '%s' % df.name,
            'datapoints': list(zip(values, timestamps))}


def dataframe_to_json_table(df):
    """Возвращает датафрейм в виде таблицы. Таблица:
    {
    "columns":[
      {"text":"Time","type":"time"},
      {"text":"Country","type":"string"},
      {"text":"Number","type":"number"}
    ],
    "rows":[
      [1234567,"SE",123],
      [1234567,"DE",231],
      [1234567,"US",321]
    ],
    "type":"table"
  }"""
    response = []
    if df.empty:
        return response
    if isinstance(df, pd.DataFrame):
        response.append({'type': 'table',
                         'columns': df.columns.map(lambda col: {"text": col}).tolist(),
                         'rows': df.where(pd.notnull(df), None).values.tolist()})
    else:
        abort(404, Exception('Received object is not a dataframe.'))
    return response


def parse_optional(target):
    res = defaultdict(lambda: None)
    if target.get('data', None) is not None:
        res['index_col'] = target['data'].get('index_col', None)
        res['log_scale'] = target['data'].get('log_scale', None)
    return res


class QueryHandler(QueryHandlerInterface):
    """Обработчик запросов для курсовой :3"""
    special_metrics = ['agreement_count', 'default_rate', #'client_count',
                            'avg_PD', 'gini']

    def __init__(self, path, filetypes: list):
        """filetypes - список обрабатываемых типов файлов"""
        self.path = path
        self.filetypes = filetypes
        self.type_class_dict = parse_types(filetypes)  # словарь {тип: класс_обработчика}
        self.file_class_dict = {}  # словарь {файл: класс_обработчика}

    def get_sources(self, d_type: str, folder: str) -> list:
        """Возвращает список ресурсов, а заодно заполняет словарь file_class_dict"""
        results = []
        for filename in os.listdir(os.path.join(self.path, folder)):
            for filetype in self.filetypes:
                if filetype in filename:
                    results.append(filename)
                    self.file_class_dict[filename] = self.type_class_dict[filetype](filename, os.path.join(self.path, folder))
                    break
        return results

    def get_metrics(self, d_type: str, source: str, target: str = '') -> list:
        columns = self.file_class_dict[source].get_columns(target)
        all_metrics = list(map(lambda x: 'special:'+x, self.special_metrics)) + columns
        return all_metrics

    def special_metric_handle(self, metric, source, options: dict):
        if metric == 'agreement_count':
            data = self.file_class_dict[source].get_data_by_column('id', options)
            data.sort_index()
            res = data.groupby(data.index).count()
            res.name = metric
            return res
        # elif metric == 'client_count':
        elif metric == 'default_rate':
            data_def = self.file_class_dict[source].get_data_by_column('default_12m', options)
            data_non_def = self.file_class_dict[source].get_data_by_column('cur_default', options)
            res = data_def.groupby(data_def.index).sum() / data_non_def.groupby(data_non_def.index).sum()
            res.name = metric
            return res
        elif metric == 'avg_PD':
            data = self.file_class_dict[source].get_data_by_column('pd', options)
            res = data.groupby(data.index).mean()
            res.name = metric
            return res
        elif metric == 'gini':
            data = self.file_class_dict[source].get_data_by_column(['pd', 'default_12m'], options)
            res = 2 * data.groupby(data.index).apply(lambda group: roc_auc_score(group.default_12m, group.pd)) - 1
            res.name = metric
            return res


    def get_data(self, request) -> list:
        results = []
        for target in request['targets']:
            source = target['source']
            if source == "":
                continue
            metric = target['target']
            if ':' in metric:
                metric = metric.split(':')[1]
            options = parse_optional(target)
            if metric in self.special_metrics:
                data = self.special_metric_handle(metric, source, options)
            else:
                data = self.file_class_dict[source].get_data_by_column(metric, options)
            if target.get('type', 'ts') == 'table':
                results.extend(dataframe_to_json_table(data))
            else:
                results.extend(dataframe_to_response(target, data))
        return results

