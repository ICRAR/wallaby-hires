"""
Start date: 10/04/24
Description: All functions neeeded for WALLABY hires pipeline
"""

# Importing all the required libraries 
from typing import BinaryIO
import numpy as np 

import os
import sys
import logging
import json
import urllib
import asyncio
import argparse
import astropy
import configparser
from astroquery.utils.tap.core import TapPlus
from astroquery.casda import Casda
import concurrent.futures
import time
from astropy.table import Table

import urllib.request

import csv
import pandas as pd

# To un-tar the files
import tarfile

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
def parset_mixin_gayatri(parset: dict, mixin: list) -> dict:
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
            RA_string = f"{RA_hh}h{RA_mm}m{RA_ss}s"
            
            Dec = str(row[2]).strip()
            Dec_split = Dec.split(':')
            Dec_dd, Dec_mm, Dec_ss = map(str.strip, Dec_split)
            Dec_string = f"{Dec_dd}.{Dec_mm}.{Dec_ss}"
            
            Vsys = float(row[3])

            # Additional parameters
            RA_beam_string = RA_string
            Dec_beam_string = Dec_string

            # Create the desired output dictionary
            output_dict = {
                'Cimager.dataset': f"$DLG_ROOT/testdata/{name}.ms",
                'Cimager.Images.Names': f"[image.{name}]",
                'Cimager.Images.direction': f"[{RA_string},{Dec_string}, J2000]",
                'Vsys': Vsys,
                'imcontsub.inputfitscube': f"image.restored.{name}",
                'imcontsub.outputfitscube': f"image.restored.{name}.contsub",
                'linmos.names': f"[image.restored.{name}.contsub]",
                'linmos.weights': f"[weights.{name}]",
                'linmos.outname': f"image.restored.{name}.contsub_holo",
                'linmos.outweight': f"weights.{name}.contsub_holo",
                'linmos.feeds.centre': f"[{RA_beam_string},{Dec_beam_string}]",
               f'linmos.feeds.image.restored.{name}.contsub': '[0.0,0.0]'
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
def parset_mixin_cimager(parset: dict, mixin: list) -> dict:
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
def parset_mixin_imcontsub(parset: dict, mixin: list) -> dict:
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
def parset_mixin_linmos(parset: dict, mixin: list) -> dict:
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

# Code for calculating and plotting the difference image 
# Location on Gayatri Desktop: http://localhost:8888/notebooks/Desktop/DIA-24/comparing_images_220524.ipynb#
def calculate_difference_image(given_image, reference_image, start_channel=111, end_channel=135):
    """
    Computes the difference image between the given image and the reference image for the specified channel range.

    Parameters:
    - given_image: numpy array, the original image (expected shape: [total_channels, 1, height, width])
    - reference_image: numpy array, the reference image to subtract (expected shape: [total_channels, 1, height, width])
    - start_channel: int, the starting channel number (inclusive)
    - end_channel: int, the ending channel number (inclusive)

    Returns:
    - diff_image: numpy array, the difference image
    """
    # Checking if the input images have the same shape
    if given_image.shape != reference_image.shape:
        raise ValueError("The given image and reference image must have the same shape.")
    
    # Converting to 0-based indexing
    start_index = start_channel - 1
    end_index = end_channel - 1

    # Validating the channel range
    if start_index < 0 or end_index >= given_image.shape[0] or start_index > end_index:
        raise ValueError("Invalid channel range specified.")
    
    # Extracting the data corresponding to the specified channels
    selected_given_channels = given_image[start_index:end_index + 1, :, :, :]
    selected_reference_channels = reference_image[start_index:end_index + 1, :, :, :]
    
    # Create the difference image
    diff_image = selected_given_channels - selected_reference_channels

    # Returning the difference image
    return diff_image


# 14/10/24
# Auto-download from casda 
# https://github.com/ICRAR/wallaby-hires/blob/main/download_data_casda/casda_download_filename.py
# Code taken from: http://localhost:8888/lab/tree/dlg/testdata/casda_download_testing_code.ipynb

# Importing required modules 
import os
import sys
import logging
import json
import urllib
import asyncio
import argparse
import astropy
import configparser
from astroquery.utils.tap.core import TapPlus
from astroquery.casda import Casda
import concurrent.futures
import time 
import csv
import pandas as pd

# HIPASS Query with filename pattern 
HIPASS_QUERY_FILENAME = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename LIKE '$filename%'"
)

# URL = "https://casda.csiro.au/casda_vo_tools/tap"

# TAP Query function
def tap_query(filename):
    """
    Queries the CASDA TAP service for a given filename.
    
    Args:
    - filename (str): The name of the file to query.
    
    Returns:
    - res (astropy.Table): Table with query result (files to download).
    """ 

    # URL = "https://casda.csiro.au/casda_vo_tools/tap"
    
    query = HIPASS_QUERY_FILENAME.replace("$filename", filename)
    # print(f"TAP Query: {query}")

    casdatap = TapPlus(url="https://casda.csiro.au/casda_vo_tools/tap", verbose=False)
    job = casdatap.launch_job_async(query)
    res = job.get_results()
    # print(f"Query result: {res}")
    return res


# TAP Query function that reads from a CSV file
def tap_query_from_csv(test_catalogue):
    """
    Reads a CSV file and performs a TAP query for each entry.
    
    Args:
    - test_catalogue (str): The path to the CSV file.
    
    Returns:
    - results (list): A list of query results for each entry in the CSV.
    """
    results = []

    with open(test_catalogue, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        
        for row in csv_reader:
            filename = row['Name']  
            print(f"Querying for: {filename}")
            
            # Perform the TAP query using the filename from CSV
            result = tap_query(filename)  
            results.append(result)
    
    return results

# Latest correct download code
def download_file(url, check_exists, output, timeout, buffer=4194304):
    """
    
    """
    # Large timeout is necessary as the file may need to be staged from tape
    
    try:
        os.makedirs(output)
    except:
        pass

    if url is None:
        raise ValueError('URL is empty')

    with urllib.request.urlopen(url, timeout=timeout) as r:
        filename = r.info().get_filename()
        filepath = f"{output}/{filename}"

        # Check if file already exists, and modify the filename if necessary
        if os.path.exists(filepath):
            base, ext = filename.rsplit('_10arc_split', 1)
            counter = 2
            new_filepath = filepath
            
            # Continue incrementing the filename until a unique one is found
            while os.path.exists(new_filepath):
                new_filename = f"{base}_10arc_split_{counter}{ext}"
                new_filepath = f"{output}/{new_filename}"
                counter += 1
            filepath = new_filepath

        http_size = int(r.info()['Content-Length'])

        if check_exists:
            try:
                file_size = os.path.getsize(filepath)
                if file_size == http_size:
                    print(f"File exists, ignoring: {os.path.basename(filepath)}")
                    # File exists and is same size; do nothing
                    return filepath
            except FileNotFoundError:
                pass

        print(f"Downloading: {filepath} size: {http_size}")
        count = 0
        with open(filepath, 'wb') as o:
            while http_size > count:
                buff = r.read(buffer)
                if not buff:
                    break
                o.write(buff)
                count += len(buff)

        download_size = os.path.getsize(filepath)
        if http_size != download_size:
            raise ValueError(f"File size does not match file {download_size} and http {http_size}")

        print(f"Download complete: {os.path.basename(filepath)}")

        return filepath

# Function to un-tar files 
def untar_file(tar_file, output_dir='.'):
    """
    Extracts a tar file (.tar, .tar.gz, .tar.bz2) to the specified output directory.

    Parameters:
    - tar_file: Path to the tar file to extract.
    - output_dir: Directory where the contents will be extracted. Defaults to the current directory.
    """
    try:
        # Extract the filename without the '.tar' extension to create a new directory
        base_name = os.path.basename(tar_file).replace('.tar', '')
        extract_dir = os.path.join(output_dir, base_name)

        # Create the target directory for extraction
        os.makedirs(extract_dir, exist_ok=True)
        
        with tarfile.open(tar_file) as tar:
            tar.extractall(path=extract_dir)
            print(f"{tar_file} un-tarred to {extract_dir}")

    except Exception as e:
        print(f"Failed to untar {tar_file}: {e}")

# Latest OG casda_download code 
def casda_download(credentials, test_catalogue, output_path, timeout_seconds):
    """
    Download data from CASDA based on the provided credentials and a test catalogue.

    Parameters:
    - credentials (str): The path to the file containing CASDA credentials.
    - test_catalogue (str): The path to the CSV file containing source information.
    - output_path (str): The directory path where downloaded files will be saved.
    - timeout_seconds (int): The maximum time (in seconds) to wait for each download request.
    
    Returns:
    - None: The function saves downloaded files to the specified output path.
    """
    
    # Read credentials from the provided file
    parser = configparser.ConfigParser()
    parser.read(credentials)

    # Initialize CASDA instance
    casda = Casda(parser["CASDA"]["username"], parser["CASDA"]["password"])

    # Read the CSV file and iterate over the 'Source' column
    with open(test_catalogue, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            filename = row['Name']  # Access the 'Name' field in the CSV
            print(f"Querying for: {filename}")

            # Modify the output path dynamically based on the filename
            dynamic_output_path = os.path.join(output_path, filename)
            
            # Check if the directory exists
            if os.path.exists(dynamic_output_path):
                print(f"Folder with filename {filename} already exists. Download skipped.")
                
                continue  # Skip to the next file after checking for tar files

            # Ensure that the directory is created if it doesn't exist
            os.makedirs(dynamic_output_path)
            print(f"Saving files to: {dynamic_output_path}")

            # Perform the TAP query using the filename from the CSV
            res = tap_query(filename) 
            url_list = casda.stage_data(res, verbose=True)
            print(f"Staging data URLs for {filename}")

            # Download files concurrently
            file_list = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for url in url_list:
                    if url.endswith('checksum'):
                        continue
                    futures.append(executor.submit(download_file, url=url, check_exists=True, output=dynamic_output_path, timeout=timeout_seconds))

                for future in concurrent.futures.as_completed(futures):
                    file_list.append(future.result())

            # Once all downloads are complete, untar all tar files in the directory
            print(f"Untarring files for: {filename}")
            for file in os.listdir(dynamic_output_path):
                file_path = os.path.join(dynamic_output_path, file)
                
                # Check if the file ends with '.tar' and is a valid tar file
                if file.endswith('.tar') and tarfile.is_tarfile(file_path):
                    untar_file(file_path, dynamic_output_path)
            

# Generating .csv file for input to the bigger pipeline
def extract_ms_files(input_csv, output_csv, search_path):
    # Load the original catalogue CSV
    df = pd.read_csv(input_csv)
    
    # Prepare a list to store the output rows
    output_data = []
    
    # Iterate through each row in the input CSV
    for index, row in df.iterrows():
        # Get the HIPASS name, RA, DEC, and Vsys from the row
        name = row['Name']
        ra = row['RA']
        dec = row['DEC']
        vsys = row['Vsys']
        
        # Define the directory path to search for .ms files
        folder_path = os.path.join(search_path, name)
        
        # Check if the directory exists
        if os.path.isdir(folder_path):
            # List all files in the directory ending with .ms
            ms_files = [f for f in os.listdir(folder_path) if f.endswith('.ms')]
            
            # Add each .ms file to the output data with specified formatting
            for ms_file in ms_files:
                # Remove the .ms extension
                ms_name = ms_file.replace('.ms', '')
                # Append to output data
                output_data.append([ms_name, ra, dec, vsys])
    
    # Create a DataFrame for the output data
    output_df = pd.DataFrame(output_data, columns=['Name', 'RA', 'DEC', 'Vsys'])
    
    # Save to CSV without the header
    output_df.to_csv(output_csv, index=False, header=False) 

# Download + Process
def process_and_download_data(credentials, input_csv, output_path, timeout_seconds, search_path):
    
    # Read credentials from the provided file
    parser = configparser.ConfigParser()
    parser.read(credentials)

    # Initialize CASDA instance
    casda = Casda(parser["CASDA"]["username"], parser["CASDA"]["password"])

    # Prepare a list to store the output rows for .ms files
    output_data = []

    # Iterate through each row in the input CSV to download data
    with open(input_csv, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            name = row['Name']
            print(f"Querying for: {name}")

            # Modify the output path dynamically based on the filename
            dynamic_output_path = os.path.join(output_path, name)

            # Check if the directory exists
            if os.path.exists(dynamic_output_path):
                print(f"Folder with filename {name} already exists. Download skipped.")
                continue  # Skip to the next file if the directory already exists

            # Ensure that the directory is created if it doesn't exist
            os.makedirs(dynamic_output_path)
            print(f"Saving files to: {dynamic_output_path}")

            # Perform the TAP query using the name from the CSV
            res = tap_query(name)
            url_list = casda.stage_data(res, verbose=True)
            print(f"Staging data URLs for {name}")

            # Download files concurrently
            file_list = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for url in url_list:
                    if url.endswith('checksum'):
                        continue
                    futures.append(executor.submit(download_file, url=url, check_exists=True, output=dynamic_output_path, timeout=timeout_seconds))

                for future in concurrent.futures.as_completed(futures):
                    file_list.append(future.result())

            # Once all downloads are complete, untar all tar files in the directory
            print(f"Untarring files for: {name}")
            for file in os.listdir(dynamic_output_path):
                file_path = os.path.join(dynamic_output_path, file)

                # Check if the file ends with '.tar' and is a valid tar file
                if file.endswith('.tar') and tarfile.is_tarfile(file_path):
                    untar_file(file_path, dynamic_output_path)

    # After all downloads, perform the extraction of .ms files
    df = pd.read_csv(input_csv)
    
    for index, row in df.iterrows():
        name = row['Name']
        ra = row['RA']
        dec = row['DEC']
        vsys = row['Vsys']

        print(f"Generating I/P .csv file for {index + 1}: Name={name}, RA={ra}, DEC={dec}, Vsys={vsys}")

        folder_path = os.path.join(search_path, name)

        if os.path.isdir(folder_path):
            ms_files = [f for f in os.listdir(folder_path) if f.endswith('.ms')]

            for ms_file in ms_files:
                ms_name = ms_file.replace('.ms', '')
                output_data.append([ms_name, ra, dec, vsys])

    output_df = pd.DataFrame(output_data, columns=['Name', 'RA', 'DEC', 'Vsys'])
    output_csv_path = os.path.join(output_path, 'hipass_ms_file_details.csv')
    output_df.to_csv(output_csv_path, index=False, header=False)
    print(f"Output saved to {output_csv_path}")

    return output_csv_path

# 30/10/24
def degrees_to_hms(degrees):
    """Convert degrees to hours-minutes-seconds."""
    hours = degrees / 15.0  # Convert degrees to hours
    h = int(hours)  # Integer part of hours
    m = int((hours - h) * 60)  # Integer part of minutes
    s = (hours - h - m / 60.0) * 3600  # Seconds

    return h, m, s

def degrees_to_dms(degrees):
    """Convert degrees to degrees-minutes-seconds."""
    d = int(degrees)  # Integer part of degrees
    m = int(abs(degrees - d) * 60)  # Integer part of minutes
    s = (abs(degrees) - abs(d) - m / 60.0) * 3600  # Seconds

    return d, m, s

# 30/10/24
# HIPASS Query with filename pattern to extract RA, DEC and Vsys
HIPASS_QUERY_RA_DEC_VSYS = (
    "SELECT RAJ2000, DEJ2000, VSys FROM \"J/AJ/128/16/table2\" WHERE "
    "HIPASS LIKE '$filename'"
)

URL_2 = 'http://tapvizier.cds.unistra.fr/TAPVizieR/tap' 

def tap_query_RA_DEC_VSYS(filename):
    # Check if 'HIPASS' is in the filename and extract the portion after it
    if 'HIPASS' in filename:
        extracted_name = filename[filename.index('HIPASS') + len('HIPASS'):]  # Get the part after 'HIPASS'
        extracted_name = extracted_name.strip()  # Remove any leading or trailing whitespace
    else:
        extracted_name = filename  # If 'HIPASS' is not found, use the filename as is

    query = HIPASS_QUERY_RA_DEC_VSYS.replace("$filename", extracted_name)
    print(f"RA DEC VSYS Query: {query}")

    casdatap = TapPlus(url=URL_2, verbose=False)
    job = casdatap.launch_job_async(query)
    res = job.get_results()
    print(f"Query result: {res}")
    return res

# Query on Topcat
# select RAJ2000, DEJ2000, VSys from "J/AJ/128/16/table2" where HIPASS like 'J1318-21'

# 30/10/24: Everything happens in the same folder 
def process_and_download_data_same_folder(credentials, input_csv, processed_catalogue, timeout_seconds):
    # Read credentials from the provided file
    parser = configparser.ConfigParser()
    parser.read(credentials)

    # Initialize CASDA instance
    casda = Casda(parser["CASDA"]["username"], parser["CASDA"]["password"])

    # Prepare a list to store the output rows for .ms files
    output_data = []

    # Load the processed catalogue to check for already processed sources
    processed_catalogue = pd.read_csv(processed_catalogue) 
    processed_sources = set(processed_catalogue['Name']) 

    with open(input_csv, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            name = row['Name']
            
            # Check if the source has already been processed
            if name in processed_sources:
                print(f"{name} already processed")
                continue  # Skip to the next row if the source is processed

            print(f"Querying for: {name}")

            # Get RA, DEC, and Vsys from the query
            res = tap_query_RA_DEC_VSYS(name)  

            # Assuming res returns a DataFrame with the required values, extract them
            if not res or len(res) == 0:
                print(f"No results found for {name}. Skipping...")
                continue

            ra = res['RAJ2000'][0] 
            dec = res['DEJ2000'][0]
            vsys = res['VSys'][0]
            print(f"Retrieved RA={ra}, DEC={dec}, VSys={vsys} for {name}")

            # Convert RA and DEC from degrees to hms and dms formats
            ra_h, ra_m, ra_s = degrees_to_hms(ra)
            dec_d, dec_m, dec_s = degrees_to_dms(dec)
            print(f"Converted RA={ra_h}h {ra_m}m {ra_s:.2f}s, DEC={dec_d}° {dec_m}′ {dec_s:.2f}″ for {name}")

            # Get filenames 
            res = tap_query(name)
            url_list = casda.stage_data(res, verbose=True)
            print(f"Staging data URLs for {name}")

            files = res['filename']
            for file in files:
                file_no_tar = file.replace('.tar', '')
                print(file_no_tar)
                output_data.append([file_no_tar, f"{ra_h}: {ra_m}: {ra_s:.2f}", f"{dec_d}: {dec_m}: {dec_s:.2f}", vsys])
            
            # Stage data for download
            url_list = casda.stage_data(res, verbose=True)
            print(f"Staging data URLs for {name}")

            # Download files concurrently in the current working directory
            file_list = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(download_file, url=url, check_exists=True, output='.', timeout=timeout_seconds)
                    for url in url_list if not url.endswith('checksum')
                ]

                for future in concurrent.futures.as_completed(futures):
                    file_list.append(future.result())

            # Untar files in the current working directory
            print(f"Untarring files for: {name}")
            for file in file_list:
                if file.endswith('.tar') and tarfile.is_tarfile(file):
                    untar_file(file, '.')

    output_df = pd.DataFrame(output_data, columns=['Name', 'RA', 'DEC', 'Vsys'])
    output_csv_path = os.path.join('.', 'hipass_ms_file_details.csv')
    output_df.to_csv(output_csv_path, index=False, header=False)
    print(f"Output saved to {output_csv_path}")

    return output_csv_path
