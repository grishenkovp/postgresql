from pydantic import BaseSettings

class SettingsDB(BaseSettings):
    server_host: str
    server_port: int

    db_dialect: str
    db_driver: str
    db_username: str
    db_password: str
    db_name: str

    class Config:
        env_file = 'dev.env'
        env_file_encoding = 'utf-8'


settings = SettingsDB()