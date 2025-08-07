import sys
import os
import time

# Allow import from utility
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logger, format_seconds

def main(username, password, host, db, port, logger):
    logger.info("Starting Load Module")

    # Simulated DB logic or loading process
    logger.debug(f"Connecting to DB '{db}' at {host}:{port} as user '{username}'")

    # Simulate loading task
    logger.info("Loading data into database...")
    time.sleep(2)  # simulate some work

    logger.info("Data loaded successfully.")

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Expected 5 arguments: host db port username password")
        sys.exit(1)

    host, db, port, username, password = sys.argv[1:6]

    log_file = os.path.join(os.path.dirname(__file__), "load.log")
    logger = setup_logger(log_file)

    start_time = time.time()

    try:
        main(username, password, host, db, port, logger)
    except Exception as e:
        logger.error(f"Load module failed: {e}")
        sys.exit(1)

    end_time = time.time()
    logger.info(f"Execution Time: {format_seconds(int(end_time - start_time))}")