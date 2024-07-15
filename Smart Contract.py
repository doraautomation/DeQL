import os
import subprocess
import shutil
import uuid

class SimpleBank:
    def __init__(self, filename):
        self.user_db = filename
    
    def execute_transaction(self, username, password, db_name):
        # Open the database file
        with open(self.user_db, "r") as f:
            for line in f:
                # Split on comma to get username and password
                db_user, db_pass = line.strip().split(",")
                # Check if entered credentials match 
                if db_user == username and db_pass == password:
                    ORIGINAL_DATABASE_PATH = f"./{db_name}" 
                    database_path = self.copy_database(ORIGINAL_DATABASE_PATH)
                    query_output = os.path.join(database_path, "results.csv")
                    self.run_query(database_path, query_output)
                    # Use the correct file to check if empty
                    if self.is_csv_empty(query_output):
                        return True
                    else:
                        return False
        return False 

    def copy_database(self, original_path):
        # Create a unique copy of the database for this run
        run_id = str(uuid.uuid4())
        new_database_path = os.path.join(os.path.dirname(original_path), f"db-copy-{run_id}")
        shutil.copytree(original_path, new_database_path)
        return new_database_path
    
    def run_query(self, database_path, query_output):
        CODEQL_PATH = r"codeql\codeql.exe"  # Adjust this to your correct CodeQL path
        QUERY_FILE = r"codeql\codeql-main\codeql-main\python\ql\examples\snippets\syntax.ql"  # Adjust path as needed
        
        # Use `subprocess.run` with the `--format=csv` option
        query_cmd = [
            CODEQL_PATH,
            "database",
            "analyze",
            database_path,
            QUERY_FILE,
            "--format=csv",
            f"--output={query_output}"  # Ensure output file ends in `.csv`
        ]

        # Run the command with error handling
        try:
            subprocess.run(query_cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error: Query analysis failed with error code {e.returncode}")
            print(e)

    @staticmethod
    def is_csv_empty(file_path):
        if not os.path.exists(file_path):
            return False
        file_size = os.path.getsize(file_path)
        return file_size == 0
