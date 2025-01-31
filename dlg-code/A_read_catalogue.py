# Importing required libraries 
import csv

# Function 2: Reads the csv file + O/P: list of sources 
def read_csv_file_as_list(filename:str)->list:
    """
    Reads a CSV file and returns its contents as a list of lists.

    Parameters:
    -----------
    filename: The name of the CSV file to be read.

    Returns:
    -------
    list: A list of lists representing each row of the CSV file.
    """
    data = []  # List to store the CSV data

    # Attempt to open the CSV file
    with open(filename, 'r') as file:
        # Create a CSV reader
        reader = csv.reader(file)

        # Read each row as a list and append to the list
        for row in reader:
            data.append(row)

        # Check if the file is empty
        if not data:
            print(f"Warning: CSV file '{filename}' is empty.")
        else:
            print(f"CSV file '{filename}' successfully read.")

    return data
