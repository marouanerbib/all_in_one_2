# extract.py

import json
import logging
import os

class JSONProcessor:
    def __init__(self, input_file, log_file=None):
        # Store the input file path
        self.input_file = input_file

        # Set up logging
        if log_file:
            self.log_file = log_file
        else:
            # Fall back to something from config if you want
            # but for now let's just do a default
            self.log_file = os.path.join(os.getcwd(), "extract.log")

        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def load_json(self):
        try:
            with open(self.input_file, 'r') as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logging.error(f"Error loading JSON from {self.input_file}: {e}")
            return None

    def extract_columns(self, columns, output_file):
        data = self.load_json()
        if data is None:
            logging.error(f"No data loaded from {self.input_file}")
            return

        extracted_data = []
        if isinstance(data, list):
            for item in data:
                extracted_row = {col: item.get(col) for col in columns if col in item}
                if extracted_row:
                    extracted_data.append(extracted_row)

        if not extracted_data:
            logging.warning(f"No matching columns found in {self.input_file}")

        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w') as file:
            json.dump(extracted_data, file, indent=4)
        logging.info(f"Extracted data saved to {output_file} ({len(extracted_data)} items).")

    def process_extraction(self):
        column_sets = {
            "company_data": self.config["columns_to_extract_from_company_data"],
            "general": self.config["columns_to_extract_from_general"],
            "chart": self.config["column_to_extract_from_chart"],
            "ratio": self.config["column_to_extract_from_ratio"]
        }

        for key, columns in column_sets.items():
            input_file = os.path.join(self.config["paths"]["divide_dir"], f"{key}.json")
            output_file = os.path.join(self.config["paths"]["extracted_dir"], f"{key}_extracted.json")
            self.extract_columns(input_file, output_file, columns)

        logging.info("Column extraction completed successfully.")


