"""Модуль для добавления, генерации и сохранения данных.
Этот модуль содержит классы для создания и генерации фейковых данных,
а также для сохранения этих данных в форматах csv, xlsx, txt
и архивирование в форматах zip, 7z.
Примеры использования:
    result = FakeArchiver()
    result.gen_data(50, 10, 10)
    result.import_data("txt.txt")
    result.update_data("Hold.|Night.|Season.|Firm.|Last deep.")
    result.save_as_file("xlsx", "xlsx")
    result.get_archive("arc", "zip", 100000)
"""
from faker import Faker
from pandas import DataFrame
from io import BytesIO
import os
import zipfile
from random import randint as ri
from typing import List, Union
import logging

SPLIT_SIMBOL = "|"

logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="a",
                    format="%(asctime)s %(levelname)s %(message)s")


class UserData:
    """Класс для генерации, ручного ввода или ввода
    из файла данных в  user_file .
    """
    def __init__(self):
        self.splitsymbol = SPLIT_SIMBOL
        self.user_file: List[List] = []

    def update_data(self, string: str):
        """Добавляет данные в  user_file.
        string-- Строка данных (разбивается на ячейки,
        разделительный символ "|").
        """
        try:
            logging.info(f"user_file дополняется- {self,string}")
            user_list = string.split(self.splitsymbol)
            self.user_file.append(user_list)
            logging.info(f"user_file дополнен пользователем- {self,string}")
        except TypeError:
            logging.error(f"TypeError- {self,string}")
            raise exit()

    def gen_data(self, line: int = ri(5, 10)*10**5,
                 col: int = 10, leng: int = 10):
        """Генерирует фейковые данные и добавляет в user_file.
        line-- Количество строк для генерации данных.
        col-- Количество столбцов для генерации данных.
        leng-- Длина каждого элемента в данных.
        """
        fake = Faker()
        try:
            logging.info(f"user_file генерируется- {self,line,col,leng}")
            self.user_file.extend([[fake.text(leng) for text in range(col)]
                                  for collum in range(line)])
            logging.info(f"user_file дополнен gen_data- {self,line,col,leng}")
        except TypeError:
            logging.error(f"TypeError- {self,line,col,leng}")
            raise exit()

    def import_data(self, file_path: str):
        """Импортирует и добавляет в user_file информацию из текстового файла.
        file_path-- Имя файла.
        Строки из файла разбивается на ячейки, разделительный символ "|".
        """
        try:
            logging.info(f"user_file дополненяется- {self,file_path}")
            with open(file_path, 'r') as file:
                data = [line.split(self.splitsymbol) for line in file]
            self.user_file.extend(data)
            logging.info(f"user_file дополнен из файла- {self,file_path}")
        except FileNotFoundError:
            logging.error(f"FileNotFoundError- {self,file_path}")
            raise exit()
        except OSError:
            logging.error(f"OSError- {self,file_path}")
            raise exit()


class Saver(UserData):
    """Класс для сохранения user_file в различных форматах."""
    def _save_to_format(self, link: Union[BytesIO, str], resolution: str,
                        copy=False):
        """Сохраняет данные в формате "csv", "xlsx".
        link-- Имя файла.
        resolution-- Формат сохранения.
        copy (bool)-- Определяет, нужно ли сохранять копию данных.
        """
        try:
            db = DataFrame(self.user_file)
            resolution_dict = {"csv": db.to_csv,
                               "xlsx": db.to_excel}
            file_path = link if copy else f'{link}.{resolution}'
            resolution_dict[resolution](file_path)
        except KeyError:
            logging.error(f"KeyError- {self, link, resolution, copy}")
        except FileNotFoundError:
            logging.error(f"FileNotFoundError- {self, link, resolution, copy}")
        except OSError:
            logging.error(f"OSError- {self, link, resolution, copy}")
            raise exit()

    def _save_text(self, link, resolution, copy=False):
        """Сохраняет user_file в формате txt.
        link-- Путь к файлу или его имя.
        resolution-- Формат сохранения.
        copy-- Определяет, нужно ли сохранять копию данных.
        """
        try:
            for data in self.user_file:
                with open(f'{link}.{resolution}', 'a') as f:
                    f.write(self.splitsymbol.join(data)+"\n")
            logging.info(f"user_file записан- {self, link, resolution, copy}")
        except FileNotFoundError:
            logging.error(f"FileNotFoundError- {self, link, resolution, copy}")
            raise exit()
        except OSError:
            logging.error(f"OSError- {self, link, resolution, copy}")
            raise exit()

    def save_as_file(self, link: str = "file", resolution: str = "txt"):
        """Сохраняет user_file в формате txt.
        link-- Путь к файлу или его имя.
        resolution-- Формат сохранения.
        """
        try:
            format_dict = {"csv": self._save_to_format,
                           "xlsx": self._save_to_format,
                           "txt": self._save_text}
            format_dict[resolution](link, resolution)
            logging.info(f"user_file дополнен- {self, link, resolution}")
        except KeyError:
            logging.error(f"KeyError- {self, link, resolution}")
            raise exit()


class FakeArchiver(Saver):
    """Класс для создания архива user_fileю"""
    def _copy_f(self, resolution: str):
        """Копирует user_file в буфер данные.
        resolution-- Формат копии данных.
        """
        try:
            buffer = BytesIO()
            self._save_to_format(buffer, resolution, copy=True)
            logging.info(f"user_file скопирован- {self, resolution}")
            return (buffer, f'file.{resolution}')
        except Exception:
            logging.error(f"user_file не скопирован- {self, resolution}")
            raise exit()

    def _copy_t(self):
        """Копирует user_file в буфер данные.
        Формат копии данных- txt.
        """
        try:
            buffer = BytesIO()
            for data in self.user_file:
                buffer.write((self.splitsymbol.join(data)+"\n").encode())
            logging.info(f"user_file скопирован- {self}, txt")
            return (buffer, "file.txt")
        except Exception:
            logging.error(f"user_file не скопирован- {self}")
            raise exit()

    def get_archive(self, name: str = "archive", resolution: str = "zip",
                    max_size: Union[int, None] = None):
        """Создает архив данных в формате "xslx", "csv".
        name-- Имя архива.
        resolution-- Формат архива.
        max_size-- Максимальный размер архива.
        """

        avilible_res = ["zip", "7z"]
        try:
            avilible_res.index(resolution)
            logging.info(f"Формат архива доступен- {self, resolution}")
        except ValueError:
            logging.error(f"Формат архива недоступен- {self, resolution}")
            raise exit()
        files = [self._copy_f("xlsx"), self._copy_f("csv"), self._copy_t()]
        num_arc = 1
        size = 0
        for file in files:
            file_name = f"{name}{num_arc}.{resolution}"
            file_size = (file[0].__sizeof__())
            if max_size:
                if file_size > max_size:
                    logging.error(f"Недостаточно памяти- {size, max_size}")
                    raise exit()
                if file_size + size > max_size:
                    num_arc += 1
            logging.info(f"Запись - {file[1]} в {file_name} {file_size}")
            with zipfile.ZipFile(f"{name}{num_arc}.{resolution}",
                                 mode="a") as archive:
                if file[1] in archive.namelist():
                    logging.error(f"{file[1]} уже есть в {file_name}")
                    raise exit()
                archive.writestr(file[1], file[0].getvalue())
                size = os.path.getsize(file_name)
            size = os.path.getsize(file_name)
            logging.info(f"Закончен процесс записи, size- {size}")
