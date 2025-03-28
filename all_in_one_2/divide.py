
import json
import logging
import os


class JSONDivider:
    def __init__(self, input_file):
        # Use the provided input file directly
        self.input_file = input_file

        # Determine the base directory dynamically
        base_dir = os.path.dirname(os.path.dirname(__file__))

        # Set up a default log directory relative to the project base directory
        default_log_dir = os.path.join(base_dir, "data", "logs")
        os.makedirs(default_log_dir, exist_ok=True)

        # Configure logging automatically
        logging.basicConfig(
            filename=os.path.join(default_log_dir, "divide.log"),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def load_json(self):
        try:
            with open(self.input_file, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            logging.error(f"File {self.input_file} not found.")
        except json.JSONDecodeError:
            logging.error(f"Failed to decode JSON from {self.input_file}.")
        return None

    def divide_and_save(self, output_dir):
        data = self.load_json()
        if data is None or 'summary' not in data:
            logging.error("Invalid JSON structure or missing 'summary' key.")
            return

        financials = data['summary']

        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        for key in ["general", "chart", "ratio", "guru", "insider", "company_data", "estimate"]:
            if key in financials:
                filename = os.path.join(output_dir, f"{key}.json")
                with open(filename, 'w') as file:
                    json.dump(financials[key], file, indent=4)
                logging.info(f"Saved: {filename}")
            else:
                logging.warning(f"Key '{key}' not found in 'summary'.")

        logging.info("JSON division completed successfully.")










######################################