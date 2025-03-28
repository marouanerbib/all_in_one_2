# Updated insert.py
import pymysql
import json
import logging
import os

class DatabaseManager:
    def __init__(self):
        self.db_config = self.config["database"]
        self.log_file = os.path.join(self.config["paths"]["log_dir"], "insert.log")
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.connection = None

    def __enter__(self):
        self.connection = pymysql.connect(
            host=self.db_config["host"],
            user=self.db_config["user"],
            password=self.db_config["password"],
            database=self.db_config["database"]
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

    def bulk_insert(self, table_name, columns, rows):
        placeholders = ', '.join(['%s'] * len(columns))
        escaped_columns = [f"`{col}`" for col in columns]
        query = f"INSERT INTO {table_name} ({', '.join(escaped_columns)}) VALUES ({placeholders})"
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(query, rows)
                self.connection.commit()
            logging.info(f"Inserted {len(rows)} rows into '{table_name}'.")
        except Exception as e:
            logging.error(f"Failed to insert data into '{table_name}': {e}")
            raise


class JSONToMySQLInserter:
    def __init__(self):
        self.config = ConfigManager().get_config()
        self.input_dir = self.config["paths"]["renamed_dir"]
        self.tables = {
            "chart": "summary_chart",
            "ratio": "summary_ratio",
            "company_data": "summary_company_data",
            "general": "summary_general"
        }

    def load_json(self, file_path):
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.error(f"Error loading JSON from {file_path}: {e}")
            return None

    def insert_data(self):
        with DatabaseManager() as db_manager:
            for key, table_name in self.tables.items():
                file_path = os.path.join(self.input_dir, f"{key}_renamed.json")
                data = self.load_json(file_path)

                if not data:
                    continue

                if isinstance(data, list) and data:
                    columns = list(data[0].keys())
                    rows = [tuple(item[col] for col in columns) for item in data]
                else:
                    logging.error(f"Unsupported JSON format for {file_path}")
                    continue

                db_manager.bulk_insert(table_name, columns, rows)
        logging.info("All JSON data successfully inserted into the database.")


