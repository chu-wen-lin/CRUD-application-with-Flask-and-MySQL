import os


class DbConfig:
    host = os.getenv("db_host")
    port = int(os.getenv("db_port"))
    user = os.getenv("db_user")
    password = os.getenv("db_password")
    db = os.getenv("db_name")
    charset = 'utf8'
