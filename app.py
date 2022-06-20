from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
from utilities.tools import DbHelper
from objects.db_config import DbConfig
from collections import namedtuple

app = Flask(__name__, template_folder='templates')
ROW_DATA = namedtuple('ROW_DATA', "id, name, address, phone, update_time")
db_helper = DbHelper(**DbConfig.__dict__)


@app.route('/')
def home():
    # todo 依ID排序，前10筆(最新的)文章
    ids = []
    result = db_helper.select_data_by_ids(ids)
    rows = [ROW_DATA(*row) for row in result]
    return render_template("index.html", rows=rows, page_title="home", today=datetime.now())


@app.route('/<int:_id>', methods=['GET'])
def detail(_id: int):  # todo 以ID取得文章(R)
    row_data = db_helper.select_data_by_id(_id)
    row_data = ROW_DATA(*row_data)
    return render_template("detail.html", row_data=row_data)


@app.route('/<int:_id>/update', methods=['POST', 'GET'])
def update_row(_id: int): # todo 更新資料(U)
    if request.method == 'POST':  # 至DB更新資料、跳轉到該筆ID的頁面(即更新成功)
        _id = request.form.get("id")
        name = request.form.get("name")
        address = request.form.get("address")
        phone = request.form.get("phone")
        update_data_list = [name, address, phone]
        rs = db_helper.update_data(_id=_id, fields=update_data_list)
        if rs:  # rs == 1
            return redirect(url_for('detail', _id=_id))
        else:  # rs == 0
            return f"row data is not found. (_id={_id})"
    else:  # 更新資料表格頁面
        row_data = db_helper.select_data_by_id(_id)
        row_data = ROW_DATA(*row_data)
        return render_template("update_form.html", row_data_abc=row_data)  # 顯示目前該筆ID的資料


@app.route('/create', methods=['POST', 'GET'])
def create_row():  # todo 新增資料(C)
    if request.method == 'POST':  # 將使用者輸入的資料寫進DB
        # FIXME 若ID已存在的檢查機制
        _id = request.form.get("id")
        name = request.form.get("name")
        address = request.form.get("address")
        phone = request.form.get("phone")
        data_inserted = [_id, name, address, phone]
        db_helper.insert_data(*data_inserted)

        return redirect(url_for('detail', _id=_id))

        # 下面這樣寫網址不會改變(留在create)
        # row_data = db_helper.select_data_by_id(_id)
        # row_data = ROW_DATA(*row_data)
        # return render_template("detail.html", row_data=row_data)

    else:  # request.method == 'GET' 空白表格供使用者輸入
        return render_template("create_raw.html")


@app.route('/<int:_id>/delete', methods=['POST', 'GET'])
def delete_row(_id: int):  # todo 以ID刪除資料(D)
    if request.method == 'POST':
        rs = db_helper.delete_row(_id)
        if rs == 1:  # deletion is completed
            return redirect(url_for('home'))  # 刪除完跳回首頁
        else:
            return f"deletion is failed. (_id={_id})"
    else:
        row_data = db_helper.select_data_by_id(_id)
        row_data = ROW_DATA(*row_data)
        return render_template("delete_raw.html", row_data=row_data)  # 顯示目前該筆ID的資料


@app.route('/html_sample', methods=['GET'])
def sample():
    return "<h1>hello world</h1>"


if __name__ == '__main__':
    app.run(debug=True)

# @app.route('/get_data_from_id', methods=['POST'])
# def get_data_by_id():  # todo 以ID取得文章
#     input_ids = request.values.get('input')
#     ids = input_ids.split(' ')
#     db_helper = CRUD application with Flask & MySQL(**DbConfig.__dict__)
#     result_2 = db_helper.select_data_by_id(ids, fields=['ID', 'Name', 'Phone'])
#     id = []
#     name = []
#     phone = []
#     for r in result_2:
#         id.append(r.get('ID'))
#         name.append(r.get('Name'))
#         phone.append(r.get('Phone'))
#     result = {'ID': id, 'Name': name, 'Phone': phone}
#     return result


# @app.route('/get_data_from_city', methods=['POST'])
# def get_data_by_city():  # todo 以地區取得最新10篇文章
#     input_city = request.values.get('input_city')
#     cities = input_city.split(' ')
#     db_helper = CRUD application with Flask & MySQL(**DbConfig.__dict__)
#     result_3 = db_helper.select_data_by_address(cities, fields=["`ID`", "`Name`"])
#     id = []
#     name = []
#     for r in result_3:
#         id.append(r.get('ID'))
#         name.append(r.get('Name'))
#     result = {'ID': id, 'Name': name}
#     return result
