"""
Start date: 10/04/24
Description: All functions neeeded for the WALLABY hires pipeline to process the HIPASS sources
"""

# Importing required modules

# Used here for functions to interact with the operating system such as os.path, os.mkdirs, os.getcwd etc 
import os

# Access and interact with Python runtime environment
import sys

# To parse and manipulate JSON data
import json

# To fetch or handle URLS and their contents 
import urllib
import urllib.request

# To support asynchronous programming.
import asyncio

# To parse command-line arguments 
import argparse

# Core package for astronomy & astrophysics 
import astropy
from astropy.table import Table

# To work with config files 
import configparser

# Interface with TAP (Table Access Protocol) services for querying databases
from astroquery.utils.tap.core import TapPlus

# Interface with CASDA i.e. CSIRO ASKAP Data Archive 
from astroquery.casda import Casda

# To execute tasks asynchronously with threads or processes
import concurrent.futures

# Used here to see how much time a given code segment took to run 
import time

# Used here to work with CSV files 
import csv

# Used here to work with dataframes 
import pandas as pd

# Used here to un-tar the files
import tarfile

import requests

# Used here to create a copy of a given dictionary 
import copy

# Provides a flexible framework for logging messages
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# To un-tar the files
import tarfile

def Cimager_test(input_dict):
    """
    Print each key-value pair in the input dictionary.

    Parameters:
    -----------
    - input_dict (dict): The dictionary containing key-value pairs to be printed.
    
    Returns:
    --------
    None
    """
    for key, value in input_dict.items():
        print(f"{key}: {value}")

def combine(str1, str2): 
    """
    Combine two input strings representing key-value pairs, removing conflicts and empty values.
    
    Parameters:
    -----------
    - str1 (str): The first string, with lines in "key=value" format.
    - str2 (str): The second string, also with lines in "key=value" format.
    
    Returns:
    --------
    - str: The combined key-value pairs as a single string, with conflicts resolved.
    lines1 = str1.strip().split('\n')
    lines2 = str2.strip().split('\n')
    """ 
    
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
    -----------
    - dict1 (dict): The first dictionary. 
    - dict2 (dict): The second dictionary.
    
    Returns:
    --------
    - dict: The merged dictionary.
    """
    merged_dict = static_dict.copy()
    merged_dict.update(dynamic_dict)
    return merged_dict

# Mixing parsets 
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

# 19/12/24
# Updated read_and_process which also includes the evaluation file locations
def read_and_process_csv(filename: str) -> list:
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

            # Read the evaluation file parameter
            evaluation_file = str(row[5]).strip()

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
                f'linmos.feeds.image.restored.{name}.contsub': '[0.0,0.0]',
                'linmos.primarybeam.ASKAP_PB.image': evaluation_file
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
# Location on Gayatri laptop: Desktop/DIA-24/comparing_images_220524.ipynb
def calculate_difference_image(given_image, reference_image, start_channel=111, end_channel=135):
    """
    Computes the difference image between the given image and the reference image for the specified channel range.

    Parameters:
    -----------
    - given_image: numpy array, the original image (expected shape: [total_channels, 1, height, width])
    - reference_image: numpy array, the reference image to subtract (expected shape: [total_channels, 1, height, width])
    - start_channel: int, the starting channel number (inclusive)
    - end_channel: int, the ending channel number (inclusive)

    Returns:
    --------
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

# HIPASS Query with filename pattern 
HIPASS_QUERY_FILENAME = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename LIKE '$filename%'"
)

# TAP Query function
def tap_query(filename):
    """
    Queries the CASDA TAP service for a given filename.
    
    Parameters:
    -----------
    - filename (str): The name of the file to query.
    
    Returns:
    --------
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
    
    Parameters:
    -----------
    - test_catalogue (str): The path to the CSV file.
    
    Returns:
    --------
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

# Code to download files from casda 
def download_file(url, check_exists, output, timeout, buffer=4194304):
    """
    Downloads a file from the specified URL to the given output directory. 
    If a file with the same name already exists, it increments a counter in 
    the filename to avoid overwriting.

    Parameters:
    - url (str): The URL of the file to download.
    - check_exists (bool): If True, checks if the file already exists in the 
      output directory and has the same size; skips download if so.
    - output (str): Path to the directory where the file will be saved.
    - timeout (int): The maximum time (in seconds) to wait for a server response.
    - buffer (int): Buffer size for reading data in chunks during download (default is 4MB).

    Returns:
    - str: The path of the downloaded file.
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
    -----------
    - tar_file: Path to the tar file to extract.
    - output_dir: Directory where the contents will be extracted. Defaults to the current directory.

    Returns:
    --------
    None

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

def degrees_to_hms(degrees):
    """
    Convert RA given in degrees to hours-minutes-seconds.

    Parameters:
    ----------
    - degrees (float): The angle in degrees to be converted.

    Returns:
    --------
    - tuple: A tuple (h, m, s) where:
      - h (int): Hours component of RA.
      - m (int): Minutes component of RA.
      - s (float): Seconds component of RA.  
    """
    hours = degrees / 15.0  # Convert degrees to hours
    h = int(hours)  # Integer part of hours
    m = int((hours - h) * 60)  # Integer part of minutes
    s = (hours - h - m / 60.0) * 3600  # Seconds

    return h, m, s

def degrees_to_dms(degrees):
    """
    Convert DEC given in degrees to degrees-minutes-seconds.

    Parameters:
    ----------
    - degrees (float): The angle in degrees to be converted.

    Returns:
    --------
    - tuple: A tuple (d, m, s) where:
      - d (int): Degrees component of the angle.
      - m (int): Minutes component of the angle.
      - s (float): Seconds component of the angle.
    """
    d = int(degrees)  # Integer part of degrees
    m = int(abs(degrees - d) * 60)  # Integer part of minutes
    s = (abs(degrees) - abs(d) - m / 60.0) * 3600  # Seconds

    return d, m, s

# HIPASS Query with filename pattern to extract RA, DEC and Vsys
HIPASS_QUERY_RA_DEC_VSYS = (
    "SELECT RAJ2000, DEJ2000, VSys FROM \"J/AJ/128/16/table2\" WHERE "
    "HIPASS LIKE '$filename'"
)
# Query on Topcat
# select RAJ2000, DEJ2000, VSys from "J/AJ/128/16/table2" where HIPASS like 'J1318-21'

URL_2 = 'http://tapvizier.cds.unistra.fr/TAPVizieR/tap' 
# TAP Query function to get the RA, DEC and Vsys values from Vizier table 
def tap_query_RA_DEC_VSYS(filename):
    """
    Executes a TAP query to retrieve Right Ascension (RA), Declination (DEC),
    and systemic velocity (VSYS) information based on the provided filename.
    
    Parameters:
    - filename (str): The name of the file, expected to contain 'HIPASS' if applicable.

    Returns:
    - Table: The query results in an Astropy Table format.
    """

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

# check and download data 
def process_and_download_data_same_folder(credentials, input_csv, processed_catalogue, timeout_seconds):
    """
    Processes an input catalogue to check for existing sources, queries unprocessed sources, 
    retrieves relevant data, stages files for download, and saves the processed details to a CSV.

    Parameters:
    - credentials (str): Path to the CASDA credentials file.
    - input_csv (str): Path to the input CSV file with source names.
    - processed_catalogue (str): Path to the catalogue of already processed sources.
    - timeout_seconds (int): Timeout setting in seconds for download operations.

    Returns:
    - None: Outputs the results to 'hipass_ms_file_details.csv' in the working directory.
    """
    
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
            filename_counts = {}  # Dictionary to keep track of duplicate counts for each file
            for file in files:
                # Remove the .tar extension from the filename
                file_no_tar = file.replace('.ms.tar', '')
            
                # Check if the filename already exists in the dictionary
                if file_no_tar in filename_counts:
                    # Increment the counter for this filename
                    filename_counts[file_no_tar] += 1
                    # Insert the counter before the .ms suffix
                    new_filename = f"{file_no_tar}_{filename_counts[file_no_tar]}"
                else:
                    # First occurrence of the filename, set counter to 1
                    filename_counts[file_no_tar] = 1
                    new_filename = file_no_tar  # Keep the original filename on the first occurrence
            
                print(f"File {new_filename} added to i/p for pipeline part B")
                output_data.append([new_filename, f"{ra_h}: {ra_m}: {ra_s:.2f}", f"{dec_d}: {dec_m}: {dec_s:.2f}", vsys])
            
            # COMMENT: this section on/off while testing on laptop
            
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
    output_csv = os.path.join('.', 'hipass_ms_file_details.csv')
    output_df.to_csv(output_csv, index=False, header=False)
    print(f"Output saved to {output_csv}")

# Test imager 
def imager():
    """
    Generates a unique filename for the imager output with a 'image_N.fits' format and creates the file.

    Parameters:
    - None
    
    Returns:
    - None: Prints a confirmation message with the filename created.
    """

    # Base filename
    base_name = "image"
    extension = ".fits"
    filename = f"{base_name}{extension}"
    counter = 1

    # Check if the file already exists and find the next available filename
    while os.path.exists(filename):
        counter += 1
        filename = f"{base_name}_{counter}{extension}"
    
    # Create the new file
    with open(filename, "w") as file:
        file.write("")

    print("imager step complete")
    print(f"Output file created: {filename}")

# Test imcontsub 
def imcontsub():
    """
    Generates a unique filename for the imcontsub output with a 'image_N.contsub.fits' extension and creates the file.

    Parameters:
    - None
    
    Returns:
    - None: Prints a confirmation message with the filename created.
    """

    # Base filename
    base_name = "image"
    extension = ".contsub.fits"
    filename = f"{base_name}{extension}"
    counter = 1

    # Check if the file already exists and find the next available filename
    while os.path.exists(filename):
        counter += 1
        filename = f"{base_name}_{counter}{extension}"
    
    # Create the new file
    with open(filename, "w") as file:
        file.write("")

    print("imcontsub step complete")
    print(f"Output file created: {filename}")

# Test linmos
def linmos():
    """
    Generates a unique filename for the imcontsub output with a 'image_N.contsub_holo.fits' extension and creates the file.

    Parameters:
    - None
    
    Returns:
    - None: Prints a confirmation message with the filename created.
    """

    # Base filename
    base_name = "image"
    extension = ".contsub_holo.fits"
    filename = f"{base_name}{extension}"
    counter = 1

    # Check if the file already exists and find the next available filename
    while os.path.exists(filename):
        counter += 1
        filename = f"{base_name}_{counter}{extension}"
    
    # Create the new file
    with open(filename, "w") as file:
        file.write("")

    print("linmos step complete")
    print(f"Output file created: {filename}")

# Test mosaic
def mosaic():
    """
    Generates unique filenames for the final mosaic output and corresponding weights file, 
    both with a '.10arc.final_mosaic.fits' extension, and creates each file.

    Parameters:
    - None
    
    Returns:
    - None: Prints confirmation messages with the filenames created.
    """

    # Base filename
    base_name = "image"
    extension = ".10arc.final_mosaic.fits"
    filename = f"{base_name}{extension}"
    counter = 1
    
    # Check if the file already exists and find the next available filename
    while os.path.exists(filename):
        counter += 1
        filename = f"{base_name}_{counter}{extension}"
    
    # Create the new file
    with open(filename, "w") as file:
        file.write("")

    # Repeat the same for weights 
    weights_name = 'weights'
    weights_filename = f"{weights_name}{extension}"
    weights_counter = 1

    # Check if the file already exists and find the next available filename
    while os.path.exists(weights_filename):
        weights_counter += 1
        weights_filename = f"{weights_name}_{weights_counter}{extension}"
    
    # Create the new file
    with open(weights_filename, "w") as file:
        file.write("")

    print("mosaic step complete")
    print(f"Output file created: {filename}")
    print(f"Output file created: {weights_filename}")

# Code for the downloading evaluation Files workflow

# HIPASS Query with filename pattern 
HIPASS_QUERY_FILENAME = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename LIKE '$filename%'"
)

URL = "https://casda.csiro.au/casda_vo_tools/tap"

# TAP Query function
def tap_query_filename_visibility(filename):
    """
    Queries the CASDA TAP service for a given filename.
    
    Args:
    - filename (str): The name of the file to query.
    
    Returns:
    - res (astropy.Table): Table with query result (files to download).
    """ 

    query = HIPASS_QUERY_FILENAME.replace("$filename", filename)
    print(f"TAP Query: {query}")

    casdatap = TapPlus(url=URL, verbose=False)
    job = casdatap.launch_job_async(query)
    res = job.get_results()
    print(f"Query result: {res}")
    return res

# HIPASS Query with SBID pattern 
HIPASS_QUERY_sbid = (
    "SELECT * FROM casda.observation_evaluation_file WHERE "
    "sbid = '$sbid'"
)

URL = "https://casda.csiro.au/casda_vo_tools/tap"

# TAP Query function
def tap_query_sbid_evaluation(sbid):
    """
    Queries the CASDA TAP service for a given filename.
    
    Args:
    - sbid (int): The sbid to query.
    
    Returns:
    - res (astropy.Table): Table with query result (files to download).
    """ 

    query = HIPASS_QUERY_sbid.replace("$sbid", str(sbid))  # Convert sbid to string
    print(f"TAP Query: {query}")

    casdatap = TapPlus(url=URL, verbose=False)
    job = casdatap.launch_job_async(query)
    res = job.get_results()
    print(f"Query result: {res}")
    return res

# Function to map evaluation files 
def find_evaluation_file(name, updated_vis_eval_dict):
    for key, filenames in updated_vis_eval_dict.items():
        # Check if the name is a substring of any filename
        if any(name in filename for filename in filenames):
            return key
    return None  # Default if no match is found 

# 13/12/24: Adding evaluation file download to this 
def process_and_download(credentials, input_csv, processed_catalogue, timeout_seconds, project_code):
    """
    Processes an input catalogue to check for existing sources, queries unprocessed sources, 
    retrieves relevant data, stages files for download, and saves the processed details to a CSV.

    Parameters:
    - credentials (str): Path to the CASDA credentials file.
    - input_csv (str): Path to the input CSV file with source names.
    - processed_catalogue (str): Path to the catalogue of already processed sources.
    - timeout_seconds (int): Timeout setting in seconds for download operations.
    - project_code (str): Code of the project 

    Returns:
    - None: Outputs the results to 'hipass_ms_file_details.csv' in the working directory.
    """
    
    # Read credentials from the provided file
    parser = configparser.ConfigParser()
    parser.read(credentials)
    username = parser["CASDA"]["username"]
    password = parser["CASDA"]["password"]

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

            # Inserting the download_evaluation_files (code) 
            # Step 1: Create sbid_visibility_dict 
            sbid_visibility_dict = {}
            res = tap_query_filename_visibility(name)

            obs_id_list = list(res['obs_id']) 
            obs_id_list = [str(item) for item in obs_id_list]

            visibility_list = list(res['filename'])
            visibility_list = [str(item) for item in visibility_list]
        
            for obs_id, visibility in zip(obs_id_list, visibility_list):
                sbid_visibility_dict.setdefault(obs_id, []).append(visibility)

            # Update the same dictionary by modifying keys
            sbid_visibility_dict = {key.replace('ASKAP-', ''): value for key, value in sbid_visibility_dict.items()}

            # Step 2: Create sbid_evaluation_dict from sbid_visibility_dict
            # Initialize the dictionary to store results
            sbid_evaluation_dict = {}

            # Extract unique SBIDs from sbid_visibility_dict
            unique_sbid_set = sbid_visibility_dict.keys()

            for sbid in unique_sbid_set:
                # Run the TAP query for the current SBID
                res = tap_query_sbid_evaluation(sbid)

                # Check if the result is not empty
                if len(res) > 0:
                    # Convert the result to an Astropy Table for easier processing
                    table = Table(res)

                    # Ensure the necessary columns exist
                    if "filename" in table.colnames and "filesize" in table.colnames:
                        # Find the row with the largest filesize
                        largest_file_row = table[table['filesize'].argmax()]
                        filename = largest_file_row['filename']  # Get the filename
                    else:
                        filename = None  # If columns are missing, set to None
                else:
                    filename = None  # If query result is empty, set to None

                # Add the SBID and its corresponding filename to the dictionary
                sbid_evaluation_dict[sbid] = filename

            # Convert np.str_ values to plain strings in sbid_evaluation_dict
            sbid_evaluation_dict = {key: str(value) for key, value in sbid_evaluation_dict.items()}

            # Print the two dictionaries 
            # Print the updated sbid_visibility_dict
            print("sbid_visibility_dict:")
            print(sbid_visibility_dict)
            
            # Print the updated dictionary
            print("sbid_evaluation_dict:")
            print(sbid_evaluation_dict)

            # Creating a new vis, eval dic based on the above two dictionaries 
            vis_eval_dict = {
                sbid_evaluation_dict[key]: value
                for key, value in sbid_visibility_dict.items()
            }

            # Print the updated dictionary
            print("vis_eval_dict:")
            print(vis_eval_dict)

            # Rename the values of the dict accordingly 

            # Make a deep copy of the dictionary
            updated_vis_eval_dict = copy.deepcopy(vis_eval_dict)

            # Dictionary to track the occurrence of filenames
            occurrence_count = {}

            # Iterate through the copy and rename duplicates
            for key, file_list in updated_vis_eval_dict.items():
                for i, filename in enumerate(file_list):
                    # If the filename has been seen before
                    if filename in occurrence_count:
                        occurrence_count[filename] += 1  # Increment the occurrence count
                        # Rename the file by appending _N
                        name_parts = filename.split('.ms.tar')  # Split to add suffix
                        new_name = f"{name_parts[0]}_{occurrence_count[filename]}.ms.tar"
                        file_list[i] = new_name  # Replace with the new name
                    else:
                        # If first occurrence, initialise count
                        occurrence_count[filename] = 1

            # Print the updated dictionary
            print("updated_vis_eval_dict:")
            print(updated_vis_eval_dict)

            # Step 3: Downloading the required evaluation files
            # Initialize CASDA instance
            casda = Casda(username, password)

            # Iterate through the dictionaries
            for sbid, required_filename in sbid_evaluation_dict.items():
                print(f"Processing SBID: {sbid}")

                # Remove 'ASKAP-' prefix if present
                sbid = str(sbid).replace('ASKAP-', '')

                # Fetch the DID (data identification) for the sbid and project code
                url = f"{DID_URL}?projectCode={project_code}&sbid={sbid}"
                logging.info(f"Requesting data from: {url}")
                res = requests.get(url)
                if res.status_code != 200:
                    raise Exception(f"Error fetching data: {res.reason} (HTTP {res.status_code})")

                logging.info(f"Response received: {res.json()}")

                # Filter evaluation files
                evaluation_files = [f for f in res.json() if "evaluation" in f]
                evaluation_files.sort()

                if not evaluation_files:
                    logging.warning(f"No evaluation files found for projectCode={project_code} and sbid={sbid}.")
                    return

                logging.info(f"Found evaluation files: {evaluation_files}")

                # Prepare the table for staging
                t = Table()
                t["access_url"] = [f"{EVAL_URL}{f}" for f in evaluation_files]

                # Stage files for download
                url_list = casda.stage_data(t)
                logging.info(f"Staging files: {url_list}")

                # Check which files need to be downloaded and filter by required filename
                download_url_list = []
                for url in url_list:
                    filename = url.split("?")[0].rsplit("/", 1)[1]
                    if filename == required_filename:
                        download_url_list.append(url)

                # View the download_url_list
                print("Files staged for download:")
                for idx, url in enumerate(download_url_list, start=1):
                    print(f"- link {idx}: {url}")

                # COMMENT: this section on/off while testing on laptop (DOWNLOADS evaluation files)
                # Download the required files
                # Define the download directory as the current working directory
                download_dir = os.getcwd()
                
                # Download the required files
                if download_url_list:
                    print(f"Downloading files to: {download_dir}")
                    filelist = casda.download_files(download_url_list, savedir=download_dir)
                    logging.info(f"Downloaded files: {filelist}")
                    logging.info(f"All files have been downloaded to {download_dir}.")
                else:
                    logging.warning("No files staged for download.")

            # Step 4: Untar all the evaluation files
            # Download directory would be the current working directory 
            download_dir = os.getcwd()

            # Iterating through dict values to untar each file 
            for sbid, tar_file in sbid_evaluation_dict.items():
                
                tar_path = os.path.join(download_dir, tar_file)

                tar_file_folder_name = os.path.splitext(tar_file)[0]
                
                # Create the folder if it doesn't already exist
                os.makedirs(tar_file_folder_name, exist_ok=True)
                        
                # Extract the .tar file into the folder
                with tarfile.open(tar_path, "r") as tar:
                    tar.extractall(path=tar_file_folder_name)
                        
                print(f"Extracted '{tar_file}' to '{tar_file_folder_name}'")

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
            filename_counts = {}  # Dictionary to keep track of duplicate counts for each file
            for file in files:
                # Remove the .tar extension from the filename
                file_no_tar = file.replace('.ms.tar', '')
            
                # Check if the filename already exists in the dictionary
                if file_no_tar in filename_counts:
                    # Increment the counter for this filename
                    filename_counts[file_no_tar] += 1
                    # Insert the counter before the .ms suffix
                    new_filename = f"{file_no_tar}_{filename_counts[file_no_tar]}"
                else:
                    # First occurrence of the filename, set counter to 1
                    filename_counts[file_no_tar] = 1
                    new_filename = file_no_tar  # Keep the original filename on the first occurrence
            
                print(f"File {new_filename} added to i/p for pipeline part B")
                output_data.append([new_filename, f"{ra_h}: {ra_m}: {ra_s:.2f}", f"{dec_d}: {dec_m}: {dec_s:.2f}", vsys])
            
            # COMMENT: this section on/off while testing on laptop (DOWNLOADS .ms files)
            
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

    # Creates a df with with filename, RA, DEC and System Velocity 
    output_df = pd.DataFrame(output_data, columns=['Name', 'RA', 'DEC', 'Vsys'])

    # Add an additional column i.e. the evaluation file
    # Apply the function to create the new column
    output_df['evaluation_file'] = output_df['Name'].apply(find_evaluation_file, args=(updated_vis_eval_dict,))

    # Define the suffix to append to evaluation_file for creating evaluation_file_path
    suffix = "LinmosBeamImages/akpb.iquv.square_6x6.54.1295MHz.SB32736.cube.fits"

    # Create a new column evaluation_file_path by combining evaluation_file with the suffix
    output_df['evaluation_file_path'] = output_df['evaluation_file'].apply(
        lambda x: x.replace('.tar', f"/{suffix}") if pd.notnull(x) else None
    )
    
    output_csv = os.path.join('.', 'hipass_ms_file_details.csv')
    output_df.to_csv(output_csv, index=False, header=False)
    print(f"Output saved to {output_csv}")

# test versions (without implementing download)
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

DID_URL = "https://casda.csiro.au/casda_data_access/metadata/evaluationEncapsulation"
EVAL_URL = "https://data.csiro.au/casda_vo_proxy/vo/datalink/links?ID="

def download_evaluation_files(filename, project_code, credentials):

    # Step 1: Create sbid_visibility_dict 
    sbid_visibility_dict = {}
    res = tap_query_filename_visibility(filename)

    obs_id_list = list(res['obs_id']) 
    obs_id_list = [str(item) for item in obs_id_list]

    visibility_list = list(res['filename'])
    visibility_list = [str(item) for item in visibility_list]
   
    for obs_id, visibility in zip(obs_id_list, visibility_list):
        sbid_visibility_dict.setdefault(obs_id, []).append(visibility)

    # Update the same dictionary by modifying keys
    sbid_visibility_dict = {key.replace('ASKAP-', ''): value for key, value in sbid_visibility_dict.items()}
    
    # Step 2: Create sbid_evaluation_dict from sbid_visibility_dict
    # Initialize the dictionary to store results
    sbid_evaluation_dict = {}

    # Extract unique SBIDs from sbid_visibility_dict
    unique_sbid_set = sbid_visibility_dict.keys()

    for sbid in unique_sbid_set:
        # Run the TAP query for the current SBID
        res = tap_query_sbid_evaluation(sbid)

        # Check if the result is not empty
        if len(res) > 0:
            # Convert the result to an Astropy Table for easier processing
            table = Table(res)

            # Ensure the necessary columns exist
            if "filename" in table.colnames and "filesize" in table.colnames:
                # Find the row with the largest filesize
                largest_file_row = table[table['filesize'].argmax()]
                filename = largest_file_row['filename']  # Get the filename
            else:
                filename = None  # If columns are missing, set to None
        else:
            filename = None  # If query result is empty, set to None

        # Add the SBID and its corresponding filename to the dictionary
        sbid_evaluation_dict[sbid] = filename

    # Convert np.str_ values to plain strings in sbid_evaluation_dict
    sbid_evaluation_dict = {key: str(value) for key, value in sbid_evaluation_dict.items()}

    # Print the two dictionaries 
    # Print the updated sbid_visibility_dict
    print("sbid_visibility_dict:")
    print(sbid_visibility_dict)
    
    # Print the updated dictionary
    print("sbid_evaluation_dict:")
    print(sbid_evaluation_dict)

    # Step 3: Downloading the required evaluation files
    # Read credentials from the provided file
    parser = configparser.ConfigParser()
    parser.read(credentials)
    username = parser["CASDA"]["username"]
    password = parser["CASDA"]["password"]

    # Initialize CASDA instance
    casda = Casda(username, password)

    # Iterate through the dictionaries
    for sbid, required_filename in sbid_evaluation_dict.items():
        print(f"Processing SBID: {sbid}")

        # Remove 'ASKAP-' prefix if present
        sbid = str(sbid).replace('ASKAP-', '')

        # Fetch the DID (data identification) for the sbid and project code
        url = f"{DID_URL}?projectCode={project_code}&sbid={sbid}"
        logging.info(f"Requesting data from: {url}")
        res = requests.get(url)
        if res.status_code != 200:
            raise Exception(f"Error fetching data: {res.reason} (HTTP {res.status_code})")

        logging.info(f"Response received: {res.json()}")

        # Filter evaluation files
        evaluation_files = [f for f in res.json() if "evaluation" in f]
        evaluation_files.sort()

        if not evaluation_files:
            logging.warning(f"No evaluation files found for projectCode={project_code} and sbid={sbid}.")
            return

        logging.info(f"Found evaluation files: {evaluation_files}")

        # Prepare the table for staging
        t = Table()
        t["access_url"] = [f"{EVAL_URL}{f}" for f in evaluation_files]

        # Stage files for download
        url_list = casda.stage_data(t)
        logging.info(f"Staging files: {url_list}")

        # Check which files need to be downloaded and filter by required filename
        download_url_list = []
        for url in url_list:
            filename = url.split("?")[0].rsplit("/", 1)[1]
            if filename == required_filename:
                download_url_list.append(url)

        # View the download_url_list
        print("Files staged for download:")
        for idx, url in enumerate(download_url_list, start=1):
            print(f"- link {idx}: {url}")

        # Download the required files
        # Uncomment this section for actual downloading
        # Define the download directory as the current working directory
        download_dir = os.getcwd()
        
        # Download the required files
        if download_url_list:
            print(f"Downloading files to: {download_dir}")
            filelist = casda.download_files(download_url_list, savedir=download_dir)
            logging.info(f"Downloaded files: {filelist}")
            logging.info(f"All files have been downloaded to {download_dir}.")
        else:
            logging.warning("No files staged for download.")

    # Step 4: Untar all the evaluation files that 
    # Download directory would be the current working directory 
    download_dir = os.getcwd()

    # Iterating through dict values to untar each file 
    for sbid, tar_file in sbid_evaluation_dict.items():
        
        tar_path = os.path.join(download_dir, tar_file)

        tar_file_folder_name = os.path.splitext(tar_file)[0]
        
        # Create the folder if it doesn't already exist
        os.makedirs(tar_file_folder_name, exist_ok=True)
                
        # Extract the .tar file into the folder
        with tarfile.open(tar_path, "r") as tar:
            tar.extractall(path=tar_file_folder_name)
                
        print(f"Extracted '{tar_file}' to '{tar_file_folder_name}'")
