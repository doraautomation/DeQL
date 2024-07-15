from concurrent.futures import ThreadPoolExecutor, wait
import threading
from mpi4py import MPI
import sys
import os
from smart_contract import SimpleBank
import test_input
import datetime
import subprocess
import hashlib
import shutil

filename = "userinfo.txt"
simple_bank = SimpleBank(filename)
_debug_ = True


def process_txn(element, db_name, rank):
    result = 0
    try:
        username, password, code_file = test_input.get_transaction_info(element)
        if simple_bank.execute_transaction(username, password,  db_name):
            result = 1
            print(f"Rank-{rank}: Authentication successful for transaction {element}.")
        else:
            print(f"Rank-{rank}: Authentication failed for transaction {element}.")
    except Exception as e:
        print(f"Rank-{rank}: Exception occurred - {e}")
    return result

def generate_hash(data):
    hash_object = hashlib.sha256()
    hash_object.update(str(data).encode('utf-8'))  # Ensuring the input is in the correct format
    return hash_object.hexdigest()

def get_original_hash(file_path, element):
    try:
        with open(file_path, 'r') as file:
            for line in file:
                line_element, line_hash = line.strip().split(',')
                if line_element == str(element):
                    return line_hash
    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
    print("Element not found in the file.")
    return None

def process_element(element, db_name, rank, ready, lock):
    file_path = "hashed_data.txt"
    node_hash = generate_hash(element)
    original_hash = get_original_hash(file_path, element)
    if(node_hash == original_hash):
      result = process_txn(element, db_name, rank)
    else:
       result = 0
       
    if result == 1:
        with lock:
            ready['count'] += 1
            
def store_element(element, output_path, rank, ready_to_commit, lock):
    log_file_path = os.path.join(output_path, f"log_{rank}.txt")
    username, password, file = test_input.get_transaction_info(element)
    with open(log_file_path, "a+") as fp:
         fp.write(f"log{rank} at {datetime.datetime.now()}: {element} - {username} {password} {file} \n")
    with lock:
        ready_to_commit['count'] += 1

def validate_element(element, db_name, output_path, comm, rank, size):
    accuracy = 0
    ready = {'count': 0}
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_element, element, db_name, rank, ready, lock) for _ in range(5)]
        wait(futures)

    shutil.rmtree(db_name)
    if ready['count'] >= 3:
        ac = (ready['count'] / 5) * 100
        accuracy += ac
        print(f"{accuracy}")
        print(f"Element {element} is ready. Count: {ready['count']}")
        commit_element(element, output_path, comm, rank, size, ready)

def commit_element(element, output_path, comm, rank, size, ready):
    ready_to_commit = {'count': 0}
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(store_element, element, output_path, rank, ready_to_commit, lock) for _ in range(5)]
        wait(futures)
    if ready_to_commit['count'] >= 3:
        username, password, file_name = test_input.get_transaction_info(element)
        repo_path = r'SC-TEST'
        codefile = file_name + ".py" 
        file_path = os.path.join(repo_path, codefile)
        try:
            os.chdir(os.path.expanduser("~"))
            subprocess.run(['cd', repo_path], check=True, shell=True)
            subprocess.run(['git', 'add', file_path], check=True, cwd=repo_path)
            subprocess.run(['git', 'commit', '-m',username], check=True, cwd=repo_path)
            subprocess.run(['git', 'push'], check=True, cwd=repo_path)
            print("File committed and pushed to repository.")
        except subprocess.CalledProcessError as e:
            print(f'Error occurred: {e}')
        print(f"Element {element} committed. Count: {ready_to_commit['count']}")

def process_array(array, output_path, comm, rank, size):
  for element in array:
    CODEQL_PATH = r"./codeql"
    code_file = f"File{element}" 
    code_path = f"{code_file}" 
    db_name = f"db-{element}"
    db_path = f"./{db_name}"  # Adjust this path as necessary
    command = [
        CODEQL_PATH,
        "database",
        "create",
        db_path,  # This is the path where the database will be created
        "--language=python",
        "--source-root",  # This flag is needed before specifying the source code directory
        code_path
    ]

    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()

        if process.returncode == 0:
            print(f"CodeQL database {db_name} created successfully.")
            validate_element(element, db_name, output_path, comm, rank, size)
        else:
            print(f"Error creating CodeQL database {db_name}:")
            print(stderr)
    except Exception as e:
        print("Error:", e)
        print(f"Failed to create CodeQL database {db_name}.") 

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    output_path = "./output"
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        
    txn = []
    file_path = f"{rank + 1}.txt"
    
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            file_contents = file.read().strip()
            if file_contents:
                txn = [int(x) for x in file_contents.split(',') if x.strip()]
            else:
                if _debug_:
                    print(f"File {file_path} is empty, rank {rank} will be idle.")
    else:
        if _debug_:
            print(f"No file found for rank {rank}, skipping.")

    if txn:
        if _debug_:
            sys.stdout.write(f"received_txn = {txn} received at rank {rank}\n")
            process_array(txn, output_path, comm, rank, size)
    else:
        if _debug_:
            print(f"Rank {rank} has no transactions to process and will be idle.")
   
