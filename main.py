from dotenv import load_dotenv
from os import getenv
import oracledb
import requests
import sys

load_dotenv()

class DolarService:
    
    def __init__(self, base_url: str):
        self.base_url = base_url

    def get_dolar_data(self, last_days: int) -> list:
        response = requests.get(f"{self.base_url}{last_days}")
        response.raise_for_status()
        return response.json()


class OracleRepository:

    def __init__(self, user, password, dsn):
        self.connection = oracledb.connect(
            user=user,
            password=password,
            dsn=dsn,
            mode=oracledb.AuthMode.SYSDBA
        )
        self.cursor = self.connection.cursor()

    def create_table(self):
        self.cursor.execute("""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE cotacao_dolar';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
        """)

        self.cursor.execute(""" 
            CREATE TABLE cotacao_dolar (
                id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                code VARCHAR2(10) DEFAULT 'USD',
                codein VARCHAR2(10) DEFAULT 'BRL',
                high NUMBER(10, 5),
                low NUMBER(10, 5),
                var_bid NUMBER(10, 6),
                pct_change NUMBER(10, 6),
                bid NUMBER(10, 5),
                ask NUMBER(10, 5),
                timestamp_utc NUMBER
            )
        """)

    def insert_dolar_data(self, data: dict):
        try:
            self.cursor.execute("""
                INSERT INTO cotacao_dolar (
                    high, low, var_bid, pct_change,
                    bid, ask, timestamp_utc
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7
                )
            """, (
                float(data["high"]),
                float(data["low"]),
                float(data["varBid"]),
                float(data["pctChange"]),
                float(data["bid"]),
                float(data["ask"]),
                int(data["timestamp"])
            ))
        except KeyError as e:
            print(f"Erro: campo ausente {e} no item: {data}")
            
    def commit_and_close(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()


def main():
    try:
        print("Inicializando...")

        dolar_service = DolarService(getenv("BASE_URL"))
        oracle_repo = OracleRepository(
            user="sys",
            password=getenv("ORACLE_PASSWORD"),
            dsn="localhost/xe"
        )

        print("Conectado ao banco de dados Oracle.")

        oracle_repo.create_table()
        print("Tabela criada com sucesso.")

        data = dolar_service.get_dolar_data(10)
        for item in data:
            oracle_repo.insert_dolar_data(item)

        oracle_repo.commit_and_close()
        print("Dados inseridos com sucesso.")

    except Exception as e:
        print(f"Erro inesperado: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()

#By Kaiw0