# Updated rename.py
import json
import logging
import os


class JSONColumnRenamer:
    def __init__(self):
        self.column_mapping = self.config["column_mapping"]
        self.log_file = os.path.join(self.config["paths"]["log_dir"], "rename.log")
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def load_json(self, file_path):
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.error(f"Error loading JSON from {file_path}: {e}")
            return None

    def rename_columns(self, data):
        if isinstance(data, list):
            return [{self.column_mapping.get(k, k): v for k, v in item.items()} for item in data]
        elif isinstance(data, dict):
            return {self.column_mapping.get(k, k): v for k, v in data.items()}
        else:
            logging.error("Unsupported JSON structure for renaming!")
            return None

    def process_renaming(self):
        for key in ["chart", "company_data", "general", "insider", "ratio", "estimate", "guru"]:
            input_file = os.path.join(self.config["paths"]["extracted_dir"], f"{key}_extracted.json")
            output_file = os.path.join(self.config["paths"]["renamed_dir"], f"{key}_renamed.json")

            data = self.load_json(input_file)
            if data is None:
                continue

            renamed_data = self.rename_columns(data)
            if renamed_data is None:
                continue

            with open(output_file, 'w') as file:
                json.dump(renamed_data, file, indent=4)
            logging.info(f"Renamed JSON data saved to {output_file}.")

        logging.info("Column renaming completed successfully.")

