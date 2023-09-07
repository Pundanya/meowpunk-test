import pandas as pd
import os
import psutil
from db import create_table_errors, Cheater, Error, get_session
from datetime import datetime


# Задача №2: Класс для обработки данных и сохранения в базе данных
class DataProcessor:
    def __init__(self):
        self.client_file = 'client.csv'
        self.server_file = 'server.csv'
        self.client_data = None
        self.server_data = None
        self.merged_data = None

    # Задача № 3: Декоратор для замера использования памяти
    @staticmethod
    def memory_usage(func):
        def wrapper(*args, **kwargs):
            process = psutil.Process(os.getpid())
            memory_start = process.memory_info()[0]
            result = func(*args, **kwargs)
            memory_end = process.memory_info()[0]
            memory_diff_kb = (memory_end - memory_start) // 1000
            print(f'Памяти использовано {func.__name__}: {memory_diff_kb} KB')
            return result

        return wrapper

    # Загрузка данных из CSV файлов
    def load_data_csv(self):
        self.client_data = pd.read_csv(self.client_file)
        self.server_data = pd.read_csv(self.server_file)

    # Объединение данных
    def merge_data(self):
        if not os.path.exists(self.client_file) or not os.path.exists(self.server_file):
            raise FileNotFoundError

        self.client_data.rename(columns={'description': 'json_client'}, inplace=True)
        self.server_data.rename(columns={'description': 'json_server'}, inplace=True)

        self.merged_data = pd.merge(self.server_data, self.client_data, on=['error_id'], how='inner',
                                    suffixes=('_server', '_client'))

    # Проверка времени бана
    @staticmethod
    def check_time(cheater: Cheater, player_timestamp: datetime):
        cheater_timestamp = datetime.strptime(cheater.ban_time, "%Y-%m-%d %H:%M:%S")
        player_timestamp = player_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        return cheater_timestamp < player_timestamp

    # Исключение из выборки записей на основе данных из базы читеров
    def remove_cheaters(self, cheaters):
        for cheater in cheaters:
            player = self.merged_data.loc[self.merged_data['player_id'] == cheater.player_id]
            if not player.empty and self.check_time(cheater, datetime.fromtimestamp(player.timestamp_server.values[0])):
                self.merged_data.drop(player.index, inplace=True)

    # Сохранение данных в базу данных
    def save_to_db(self):
        self.merged_data.rename(columns={"timestamp_server": "timestamp"}, inplace=True)
        data_to_insert = self.merged_data.to_dict(orient='records')
        with get_session() as session:
            session.bulk_insert_mappings(Error, data_to_insert)
            session.commit()

    # Основная обработка данных
    @memory_usage
    def process_data(self):
        try:
            self.load_data_csv()
        except FileNotFoundError as exc:
            raise RuntimeError('Ошибка загрузки данных из CSV') from exc
        self.merge_data()
        try:
            with get_session() as session:
                cheaters = session.query(Cheater).all()
        except ConnectionError as exc:
            raise RuntimeError('Ошибка подключения к БД') from exc
        self.remove_cheaters(cheaters)
        self.save_to_db()


if __name__ == "__main__":
    # Задача №1: Создание в sqlite пустой таблицы с полями
    create_table_errors()
    # Обработка данных и сохранение в базе данных
    data_processor = DataProcessor()
    data_processor.process_data()
