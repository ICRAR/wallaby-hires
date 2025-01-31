"""
Start date: 28/02/24 
Function: To read and process the catalogue   
"""

# Importing required libraries 
import csv

def read_and_process_csv_file(filename: str) -> list:
    """
    Reads a CSV file and processes its contents, returning a list of dictionaries.

    Parameters:
    -----------
    filename: The name of the CSV file to be read.

    Returns:
    --------
    list: A list of dictionaries representing each processed row of the CSV file.
    """

    # List to store the source dictionary 
    data = []  

    # try:
        # Opening the .csv file 
    with open(filename, 'r') as file:
        # Create a CSV reader
        reader = csv.reader(file)
        
        # Read and process each row, including the header
        for row in reader:
            
            # Extract individual parameters

            # Extract name 
            name = str(row[0]).strip()

            # Extract RA 
            RA = str(row[1]).strip()
            RA_split = RA.split(':')
            RA_hh, RA_mm, RA_ss = map(str.strip, RA_split)
            RA_string = RA_hh + "h" + RA_mm + "m" + RA_ss
        
            # Extract DEC 
            Dec = str(row[2]).strip()
            Dec_split = Dec.split(':')
            Dec_dd, Dec_mm, Dec_ss = map(str.strip, Dec_split)
            Dec_string = Dec_dd + "d" + Dec_mm + "m" + Dec_ss + "s"
        
            # Extract System Velocity 
            Vsys = float(row[3])

            # Create a dictionary for this source
            source_dict = {'name': name, 'RA_string': RA_string, 'Dec_string': Dec_string, 'Vsys': Vsys}
            
            data.append(source_dict)

        # Check if the file is empty
        if not data:
            print(f"Warning: CSV file '{filename}' is empty.")
        else:
            print(f"CSV file '{filename}' successfully read and processed into a list of dictionaries.")
            
    # except FileNotFoundError:
    #     # Handle the case where the file is not found
    #     print(f"Error: CSV file '{filename}' not found.")
    # except Exception as e:
    #     # Handle unexpected errors
    #     print(f"Error: An unexpected error occurred - {e}")
    #     raise e

    return data