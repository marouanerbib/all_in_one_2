import os
import logging
from all_in_one_2.divide import JSONDivider
from all_in_one_2.extract import JSONProcessor
from all_in_one_2.rename import JSONColumnRenamer
from all_in_one_2.insert import JSONToMySQLInserter
from all_in_one_2.config import load_config


def setup_logging():
    # Set up logging to the data/logs directory
    base_dir = os.path.dirname(os.path.dirname(__file__))  # Parent directory of the script
    log_dir = os.path.join(base_dir, "data", "logs")
    os.makedirs(log_dir, exist_ok=True)  # Create logs directory if it doesn't exist
    log_file = os.path.join(log_dir, "pipeline.log")

    # Configure logging
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def generate_output_paths(base_dir, input_filename):
    """
    Génère tous les chemins nécessaires pour chaque type de donnée (general, chart, etc.)
    dans les répertoires : code_devide, code_extract et code_rename.
    """
    categories = [
        "general", "chart", "ratio", "guru", "insider", "company_data", "estimate"
    ]

    output_paths = {}

    for category in categories:
        # Fichiers divisés
        output_paths[f"divided_{category}_file"] = os.path.join(
            base_dir, "data", "code_devide", f"{category}.json"
        )

        # Fichiers extraits
        output_paths[f"extracted_{category}_file"] = os.path.join(
            base_dir, "data", "code_extract", f"{input_filename}_{category}_extracted.json"
        )
        # Fichiers renommés
        output_paths[f"renamed_{category}_file"] = os.path.join(
            base_dir, "data", "code_rename", f"{input_filename}_{category}_renamed.json"
        )

        # Créer les dossiers si besoin
        os.makedirs(os.path.dirname(output_paths[f"divided_{category}_file"]), exist_ok=True)
        os.makedirs(os.path.dirname(output_paths[f"extracted_{category}_file"]), exist_ok=True)
        os.makedirs(os.path.dirname(output_paths[f"renamed_{category}_file"]), exist_ok=True)

    return output_paths




def main():
    setup_logging()
    config = load_config()

    base_dir = os.path.dirname(os.path.dirname(__file__))
    input_filename = os.path.splitext(os.path.basename(config["input"]["json_file"]))[0]
    output_paths = generate_output_paths(base_dir, input_filename)

    try:
        # Step 1: Divider
        divide_directory = os.path.join(base_dir, "data", "code_devide")
        divider = JSONDivider(config["input"]["json_file"])
        divider.load_json()
        #divider.divide_and_save(output_paths["divide_dir"])
        divider.divide_and_save(divide_directory)
        logging.info("JSON divided successfully.")
        print(f"Contents of divide directory: {os.listdir(divide_directory)}")
        #print(f"Contents of divide directory: {os.listdir(output_paths['divide_dir'])}")
        # Les catégories à traiter
        categories = [
            "general", "chart", "ratio", "company_data"
        ]

        # Step 2 & 3 : Extract & Rename
        for category in categories:
            divided_file = output_paths[f"divided_{category}_file"]
            extracted_file = output_paths[f"extracted_{category}_file"]
            renamed_file = output_paths[f"renamed_{category}_file"]

            # Clé dynamique dans le config : columns_to_extract_from_general, etc.
            extract_key = f"columns_to_extract_from_{category}"

            if extract_key not in config:
                logging.warning(f"No columns to extract defined for category: {category}")
                continue

            # Extraction
            extractor = JSONProcessor(divided_file, "logs/extraction.log")
            extractor.load_json()
            extractor.extract_columns(
                config[extract_key],
                extracted_file
            )
            logging.info(f"Columns extracted for {category}")

            # Renommage
            renamer = JSONColumnRenamer(extracted_file, renamed_file)
            renamer.load_json()
            renamer.rename_columns(config["column_mapping"])
            renamer.save_json()
            logging.info(f"Columns renamed for {category}")

        # Step 4: Insert data into MySQL for each category
        for category in categories:
            renamed_file = output_paths[f"renamed_{category}_file"]
            table_name = f"summary_{category}"  # Add prefix "summary_"

            inserter = JSONToMySQLInserter(
                config["database"],
                renamed_file,
                table_name
            )
            inserter.insert_data()
            logging.info(f"Data inserted into MySQL successfully into table: {table_name}")

        print("Pipeline executed successfully.")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()