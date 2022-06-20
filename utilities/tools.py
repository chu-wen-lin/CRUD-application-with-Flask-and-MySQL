import csv
import glob
from contextlib import contextmanager
import pymysql
import datetime
from typing import List
from typing import Dict, Generator
from objects.db_config import DbConfig


def get_path(directory: str) -> List[str]:  # // get a list of file paths
    return glob.glob(directory + '/*.csv')


def read_csv_file(file_list: List[str]) -> List:  # // read csv files return the data
    each_file_rows = []
    for file in file_list:
        with open(file, newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                each_file_rows.append(row)
    return each_file_rows
    # each_file_rows = [{'地址全址':'...', '機構電話':'...'},{: , : , ...}...]


class DbHelper:
    def __init__(self, host, port, user, password, db, charset, **kwargs):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset

    @contextmanager
    def get_connection(self):
        connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db,
            charset=self.charset
        )
        try:
            yield connection
        finally:
            connection.close()

    def create_table(self, table_name):
        with self.get_connection() as connect:
            cursor = connect.cursor()

            create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` (`ID` INT NOT NULL AUTO_INCREMENT,`Name` VARCHAR(200), `Address` VARCHAR(200), `Phone` VARCHAR(200), `UpdateTime` DATETIME, PRIMARY KEY (`ID`))"
            cursor.execute(create_table_query)

            connect.commit()

    def add_column(self, name):
        with self.get_connection() as connect:
            cursor = connect.cursor()

            add_column_query = f"ALTER TABLE Document ADD {name} DATETIME"
            cursor.execute(add_column_query)

            connect.commit()

    def initialize_data(self, name, address, phone):
        with self.get_connection() as connect:
            cursor = connect.cursor()

            insert_query = "INSERT INTO covid_labeled.Document (`Name`, `Address`, `Phone`,`UpdateTime`) VALUES(%s, %s, %s, %s)"  # 用{name} {address} {phone}遇到跳脫字元會有問題
            cursor.execute(insert_query, (name, address, phone, datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))

            connect.commit()

    def insert_data(self, id, name, address, phone):
        with self.get_connection() as connect:
            cursor = connect.cursor()

            insert_query = "INSERT INTO covid_labeled.Document (`id`,`Name`, `Address`, `Phone`,`UpdateTime`) VALUES(%s, %s, %s, %s, %s)"  # 用{name} {address} {phone}遇到跳脫字元會有問題
            cursor.execute(insert_query, (id, name, address, phone, datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')))

            connect.commit()


    def select_data_by_address(self, cities: List[str], fields: List[str] = '*') -> Dict:
        """
        :param cities: 篩選的縣市列表
        :param fields: 需要的欄位列表
        :return: 資料庫查詢結果
        """
        with self.get_connection() as connect:
            cursor = connect.cursor()
            # 臺南、高雄、屏東的最新10筆
            if cities and len(cities) > 0:
                condition = ' WHERE ' + ' or '.join([f'`Address` Like "%%{city}%%"' for city in cities])
            else:
                condition = ""

            select_query = f"SELECT {', '.join(fields)} FROM Document {condition} ORDER BY ID DESC limit 10"
            cursor.execute(select_query)
            data = cursor.fetchall()
            # data : ((20783, '臺南市白河區大林社區發展協會'), ('ID', 'Address'), ...)

            for d in data:
                yield {field.strip('`'): d[i] for i, field in enumerate(fields)}

            connect.commit()

    def select_data_by_id(self, _id: str) -> tuple:
        with self.get_connection() as connect:
            cursor = connect.cursor()

            select_query = f"SELECT * FROM Document WHERE ID={_id}"
            cursor.execute(select_query)
            data = cursor.fetchall()
            print(data)
            connect.commit()
            return data[0]
        # return [row for row in self.select_data_by_ids(ids=[_id])][0]

    def select_data_by_ids(self, ids: List[str], fields: List[str] = '*') -> Generator:  # = ' * ' :default
        """

        :param ids:
        :param fields:
        :return:
        """
        with self.get_connection() as connect:
            cursor = connect.cursor()

            if ids and len(ids) > 0:
                condition = ' WHERE ' + ' or '.join([f" `id` = '{id}'" for id in ids])
            else:
                condition = ""

            select_query = f"SELECT {', '.join(fields)} FROM Document {condition} ORDER BY ID DESC limit 10"
            cursor.execute(select_query)
            data = cursor.fetchall()

            for d in data:
                # print(d)
                yield d

            connect.commit()

    def update_address(self):  # // add postal code
        file = '/Users/chuwen/Desktop/eland/postal_code/postal_code.csv'
        with open(file, newline='') as csvfile:
            postal_code_file = csv.reader(csvfile, delimiter=',')
            postal_code_list = []
            for d in postal_code_file:
                postal_code_list.append(d)
        postal_code_list.pop(0)
        postal_code_dict = {}
        for data in postal_code_list:
            postal_code_dict[data[0] + data[1]] = data[2]

            with self.get_connection() as connect:
                cursor = connect.cursor()

                # 取得所有地址資料
                get_all_address_query = "SELECT Address from Document"
                cursor.execute(get_all_address_query)
                results = cursor.fetchall()

                address_with_postal_code = []
                for result in results:
                    if result[0][0:5] in postal_code_dict.keys():
                        address_with_postal_code.append(postal_code_dict[result[0][0:5]] + " " + result[0])
                    elif result[0][0:4] in postal_code_dict.keys():
                        address_with_postal_code.append(postal_code_dict[result[0][0:4]] + " " + result[0])
                    elif result[0][0:6] in postal_code_dict.keys():
                        address_with_postal_code.append(postal_code_dict[result[0][0:6]] + " " + result[0])

                # 覆蓋資料庫的資料、加入更新時間
                for i, addr in enumerate(address_with_postal_code):
                    update_query = "UPDATE Document SET Address = %s , UpdateTime = %s WHERE ID = %s "
                    cursor.execute(update_query, (addr, datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'), i + 1))

                connect.commit()

    def update_data(self, _id: int, fields: List):
        with self.get_connection() as connect:
            cursor = connect.cursor()
            update_query = "UPDATE Document SET Name=%s, Address=%s, Phone=%s, UpdateTime=%s WHERE ID=%s"
            try:
                rs = cursor.execute(update_query, (
                    fields[0], fields[1], fields[2], datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'), _id))
            except pymysql.err.OperationalError as e:
                print(e)
                return e.__str__()
            connect.commit()
            return rs

    def delete_row(self, _id: str):
        with self.get_connection() as connect:
            cursor = connect.cursor()

            delete_query = f"DELETE FROM Document WHERE id={_id}"
            try:
                rs = cursor.execute(delete_query)
                connect.commit()
                return rs
            except Exception as e:
                print(e)
                return e

    def delete_rows(self, ids: List[str]):
        with self.get_connection() as connect:
            cursor = connect.cursor()

            if ids and len(ids) > 0:
                condition = ' WHERE ' + ' or '.join([f" `id` = {id}" for id in ids])
            else:
                condition = ''

            delete_query = f"DELETE FROM Document {condition}"
            cursor.execute(delete_query)

            connect.commit()


if __name__ == '__main__':
    db_helper = DbHelper(**DbConfig.__dict__)

    # // set the directory
    # directory = '/Users/chuwen/Desktop/eland'

    # // get file_list
    # file_list = get_path(directory)

    # // get rows from csv
    # each_file_rows = read_csv_file(file_list)

    # // create table named document_2
    # db_helper = CRUD application with Flask & MySQL(**DbConfig.__dict__)
    # db_helper.create_table('document_2')

    # // initialize data
    # for data in each_file_rows:
    #     name = data['\ufeff"機構名稱"']
    #     address = data['地址全址']
    #     phone = data['機構電話']
    #     data_inserted = [name, address, phone]
    #
    #     try:
    #         db_helper.initialize_data(*data_inserted)
    #
    #     except Exception as e:
    #         print("-" * 10)
    #         print(e)  # error message
    #         print("\n".join(data_inserted))

    # db_helper = CRUD application with Flask & MySQL(**DbConfig.__dict__)
    # data_inserted = ['香蕉托兒所', '台北市火箭區', '02-1234']
    # data_inserted = ['山竹幼兒園', '南極洲冰塊A', '99999']
    # data_inserted = ['橘子菜市場', '北極海', '02345677']
    # data_inserted = ['火龍果幼稚園', '火鳥星雲', '2222222']
    # db_helper.initialize_data(*data_inserted)

    # // Select data by address
    # cities = ['臺南', '高雄', '屏東']
    # result = db_helper.select_data_by_address(cities, fields='*')
    # for r in result:
    #     print(r)

    # // Select data by id
    # db_helper.select_data_by_id(20788)

    # // Select data by ids
    # ids = ['2266', '135', '68']
    # db_helper = CRUD application with Flask & MySQL(**DbConfig.__dict__)
    # result = db_helper.select_data_by_id(ids, fields=['ID', 'Name', 'Phone'])
    # for r in result:
    #     print(r)

    # Select the newest 10 rows
    # ids = []
    # result_1 = db_helper.select_data_by_ids(ids)
    # for r in result_1:
    #     print(r)

    # # // Update data
    # db_helper = CRUD application with Flask & MySQL(**DbConfig.__dict__)
    # fields = ['香蕉幼稚園', '臺北市大安區', '02-2222222']
    # db_helper.update_data(15, fields)

    # // Add a column
    # add_column('Update_Time')

    # // Delete row
    # db_helper = CRUD application with Flask & MySQL(**DbConfig.__dict__)
    # _id = 6
    # db_helper.delete_row(_id)

    # // Delete multiple rows
    # ids = [1, 10]
    # db_helper = CRUD application with Flask & MySQL(**DbConfig.__dict__)
    # db_helper.delete_rows(ids)

    # TODO :
    #  不要一次抓所有資料下來改完再塞回去，select...update...
    #  使用yield，節省記憶體
    #  {fields}讓select彈性
    #  將所有輸入值都放在main下
    #  更改變數名稱(駝峰式或底線；除了class外，大多小寫開頭)