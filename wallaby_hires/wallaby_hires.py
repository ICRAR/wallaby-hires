"""
Start date: 10/04/24

Description: All functions neeeded for WALLABY hires pipeline

"""

# Importing required libraries 
import csv
from typing import BinaryIO


def Cimager_test(input_dict):
    for key, value in input_dict.items():
        print(f"{key}: {value}")

def combine(str1, str2):
    lines1 = str1.strip().split('\n')
    lines2 = str2.strip().split('\n')
    
    # Create dictionaries from lines
    dict1 = {line.split('=')[0]: line.split('=')[1] for line in lines1 if line.strip()}
    dict2 = {line.split('=')[0]: line.split('=')[1] for line in lines2 if line.strip()}
    
    # Combine dictionaries
    combined_dict = {}
    for key, value in dict1.items():
        if value.strip() != '' and value.strip() != 'None':
            combined_dict[key] = value
    for key, value in dict2.items():
        if key not in combined_dict or (value.strip() != '' and value.strip() != 'None'):
            combined_dict[key] = value
    
    # Remove conflicting occurrences
    for key in combined_dict.keys():
        if '[' in key and ']' in key:
            base_key = key.split('[')[0]
            for k in combined_dict.keys():
                if base_key == k.split('[')[0] and '[' not in k:
                    del combined_dict[k]
    
    # Convert dictionary back to string
    result = '\n'.join([f"{key}={value}" for key, value in combined_dict.items()])
    return result

# Merge dictionaries 
def merge_dictionaries(static_dict, dynamic_dict)->dict:
    """
    Merge two dictionaries.
    
    Parameters:
    - dict1 (dict): The first dictionary. 
    - dict2 (dict): The second dictionary.
    
    Returns:
    - dict: The merged dictionary.
    """
    merged_dict = static_dict.copy()
    merged_dict.update(dynamic_dict)
    return merged_dict




# Andreas's function 
def parset_mixin(parset: dict, mixin: dict) -> bytes:
    """
    Update parset with dict values.

    Parameters:
    -----------
    parset: standard parset dictionary
    mixin: Could be a plain key-value dictionary or another parset.

    Returns:
    --------
    binary encoded YANDA parset
    """
    for key, value in mixin.items():
        if key in parset:
            parset[key]["value"] = value
        else:
            parset.update({key: {"value": value, "type": "string", "description": ""}})
    serialp = "\n".join(f"{x}={y['value']}" for x, y in parset.items())
    return serialp.encode("utf-8")

# Reading sources from the catalogue and returning each source in a list of dynamic parset dictionaries 
def read_and_process_csv_file_output_dynamic_parset(filename: str) -> list:
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

    # Opening the .csv file 
    with open(filename, 'r') as file:
        # Create a CSV reader
        reader = csv.reader(file)
        
        # Read and process each row, including the header
        for row in reader:
            
            # Extract individual parameters
            name = str(row[0]).strip()
            RA = str(row[1]).strip()
            RA_split = RA.split(':')
            RA_hh, RA_mm, RA_ss = map(str.strip, RA_split)
            RA_string = f"{RA_hh}:{RA_mm}:{RA_ss}"
            
            Dec = str(row[2]).strip()
            Dec_split = Dec.split(':')
            Dec_dd, Dec_mm, Dec_ss = map(str.strip, Dec_split)
            Dec_string = f"{Dec_dd}:{Dec_mm}:{Dec_ss}"
            
            Vsys = float(row[3])

            # Create the desired output dictionary
            output_dict = {
                'Cimager.dataset': f"$DLG_ROOT/testdata/{name}",
                'Cimager.Images.Names': f"[image.{name}]",
                'Cimager.Images.direction': f"[{RA_string},{Dec_string} J2000]",
                'Vsys': Vsys
            }
            
            data.append(output_dict)

        # Check if the file is empty
        if not data:
            print(f"Warning: CSV file '{filename}' is empty.")
        else:
            print(f"CSV file '{filename}' successfully read and processed into a list of dictionaries.")

    return data

# static and dynamic parset mixing 
def parset_mixin_gayu(parset: dict, mixin: list) -> dict:
    """
    Update parset with values from a list of dictionaries.

    Parameters:
    -----------
    parset: standard parset dictionary
    mixin: List of dictionaries containing key-value pairs to update parset

    Returns:
    --------
    Updated parset dictionary
    """
    for item in mixin:
        for key, value in item.items():
            if key in parset:
                parset[key]["value"] = value
            else:
                parset.update({key: {"value": value, "type": "string", "description": ""}})

    serialp = "\n".join(f"{x}={y['value']}" for x, y in parset.items())
    return serialp.encode("utf-8")

# 11/04/24 
# Updated read & process where it generates a combined dynamic parset 
def read_and_process_csv_file_output_all_dynamic_parset(filename: str) -> list:
    """
    Reads a CSV file and processes its contents, returning a list of dictionaries.

    Parameters:
    -----------
    filename: The name of the CSV file to be read.

    Returns:
    -------
    list: A list of dictionaries representing each processed row of the CSV file.
    """

    # List to store the source dictionary 
    data = []  

    # Opening the .csv file 
    with open(filename, 'r') as file:
        # Create a CSV reader
        reader = csv.reader(file)
        
        # Read and process each row, including the header
        for row in reader:
            
            # Extract individual parameters
            name = str(row[0]).strip()
            RA = str(row[1]).strip()
            RA_split = RA.split(':')
            RA_hh, RA_mm, RA_ss = map(str.strip, RA_split)
            RA_string = f"{RA_hh}:{RA_mm}:{RA_ss}"
            
            Dec = str(row[2]).strip()
            Dec_split = Dec.split(':')
            Dec_dd, Dec_mm, Dec_ss = map(str.strip, Dec_split)
            Dec_string = f"{Dec_dd}:{Dec_mm}:{Dec_ss}"
            
            Vsys = float(row[3])

            # Additional parameters
            RA_beam_string = RA_string
            Dec_beam_string = Dec_string

            # Create the desired output dictionary
            output_dict = {
                'Cimager.dataset': f"$DLG_ROOT/testdata/{name}.ms",
                'Cimager.Images.Names': f"[image.{name}]",
                'Cimager.Images.direction': f"[{RA_string},{Dec_string} J2000]",
                'Vsys': Vsys,
                'imcontsub.inputfitscube': f"image.restored.{name}",
                'imcontsub.outputfitscube': f"image.restored.i.{name}.contsub",
                'linmos.names': f"[image.restored.i.{name}.contsub]",
                'linmos.weights': f"[weights.i.{name}]",
                'linmos.outname': f"image.restored.i.{name}.contsub_holo",
                'linmos.outweight': f"weights.i.{name}.contsub_holo",
                'linmos.feeds.centre': f"[{RA_beam_string},{Dec_beam_string}]",
                f'linmos.feeds.image.restored.i.{name}.contsub': '[0.0,0.0]'
            }
            
            data.append(output_dict)

        # Check if the file is empty
        if not data:
            print(f"Warning: CSV file '{filename}' is empty.")
        else:
            print(f"CSV file '{filename}' successfully read and processed into a list of dictionaries.")

    
    return data

# 12/04/24 
# Combine: Cimager parset 
def parset_mixin_gayu_cimager(parset: dict, mixin: list) -> dict:
    """
    Update parset with values from a list of dictionaries where keys start with 'Cimager'.

    Parameters:
    -----------
    parset: standard parset dictionary
    mixin: List of dictionaries containing key-value pairs to update parset

    Returns:
    --------
    Updated parset dictionary, with only Cimager parameters combined 
    """
    for item in mixin:
        for key, value in item.items():
            if key.startswith("Cimager"):
                if key in parset:
                    parset[key]["value"] = value
                else:
                    parset.update({key: {"value": value, "type": "string", "description": ""}})
    
    serialp = "\n".join(f"{x}={y['value']}" for x, y in parset.items())
    return serialp.encode("utf-8")

# Combine: imcontsub parset 
def parset_mixin_gayu_imcontsub(parset: dict, mixin: list) -> dict:
    """
    Update parset with values from a list of dictionaries where keys start with 'imcontsub'.

    Parameters:
    -----------
    parset: standard parset dictionary
    mixin: List of dictionaries containing key-value pairs to update parset

    Returns:
    --------
    Updated parset dictionary
    """
    for item in mixin:
        for key, value in item.items():
            if key.startswith("imcontsub"):
                if key in parset:
                    parset[key]["value"] = value
                else:
                    parset.update({key: {"value": value, "type": "string", "description": ""}})
                        
    serialp = "\n".join(f"{x}={y['value']}" for x, y in parset.items())
    return serialp.encode("utf-8")

# Combine: linmos parset
def parset_mixin_gayu_linmos(parset: dict, mixin: list) -> dict:
    """
    Update parset with values from a list of dictionaries where keys start with 'linmos'.

    Parameters:
    -----------
    parset: standard parset dictionary
    mixin: List of dictionaries containing key-value pairs to update parset

    Returns:
    --------
    Updated parset dictionary
    """
    for item in mixin:
        for key, value in item.items():
            if key.startswith("linmos"):
                if key in parset:
                    parset[key]["value"] = value
                else:
                    parset.update({key: {"value": value, "type": "string", "description": ""}})
                        
    serialp = "\n".join(f"{x}={y['value']}" for x, y in parset.items())
    return serialp.encode("utf-8") 