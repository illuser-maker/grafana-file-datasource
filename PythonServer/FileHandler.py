from abc import ABC, abstractmethod

import pandas as pd
import csv


class FileHandlerInterface(ABC):
    """Интерфейс обработчика типа фалов"""

    @abstractmethod
    def get_columns(self, target):
        pass

    @abstractmethod
    def get_data_by_column(self, column, options):
        pass


class CSVHandler(FileHandlerInterface):

    def __init__(self, filename: str, path: str, date_col=None):
        self.file = path + '/' + filename
        self.data = None
        self.date_col = None
        with open(self.file, 'r') as csv_file:
            self.dialect = csv.Sniffer().sniff(csv_file.readline())
            csv_file.seek(0)
            reader = csv.reader(csv_file, self.dialect)
            self.columns = next(reader)
            if date_col is None:
                self.date_col = self.find_date()
                if self.date_col is not None:
                    self.columns.remove(self.columns[self.date_col])
            elif date_col != -1:
                self.date_col = date_col

    def get_columns(self, target: str):
        columns = []
        for key in self.columns:
            if key.find(target) != -1:
                columns.append(key)
        return columns

    def find_date(self):
        for i, col in enumerate(self.columns):
            if ('date' in col.lower()) or ('дата' in col.lower()) or ('_dt' in col.lower()):
                return i
        return None

    def get_data_by_column(self, column, options):
        index_col = options['index_col']
        if self.data is None or index_col is not None:
            index_col = self.date_col if index_col is None else index_col
            self.data = pd.read_csv(self.file, dialect=self.dialect, index_col=index_col)
            if self.date_col is not None:
                self.data.index = pd.to_datetime(self.data.index).astype(int) / 10 ** 6

        if isinstance(self.data.loc[:, column], pd.Series) and (self.data.loc[:, column].dtype == object):
            self.data.loc[:, column] = pd.to_numeric(
                self.data.loc[:, column].str.replace(',', '.'), errors='ignore').values
        elif isinstance(self.data.loc[:, column], pd.DataFrame):
            for col in column:
                if self.data.loc[:, col].dtype == object:
                    self.data.loc[:, col] = pd.to_numeric(self.data.loc[:, col].str.replace(',', '.'),
                                                             errors='ignore').values
        return self.data.loc[:, column]




class XLSXHandler(FileHandlerInterface):
    pass


SUPPORTED_TYPES = {
    'csv': CSVHandler
    # 'xlsx': XLSXHandler
}


def parse_types(filetypes):
    """Возвращает словарь {тип файла: класс обработчика}"""
    res_dict = {}
    for filetype in filetypes:
        if filetype in SUPPORTED_TYPES:
            res_dict[filetype] = SUPPORTED_TYPES[filetype]
    return res_dict
