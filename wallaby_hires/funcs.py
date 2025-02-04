"""
Start date: 10/04/24
End date: 
Description: All functions neeeded for the WALLABY hires test & deploy pipelines to process the HIPASS sources. 
"""

# Importing required modules
import os
import sys
import json
import urllib
import urllib.request
import asyncio
import argparse
import astropy
from astropy.table import Table
from astropy import units as u
from astropy import constants
import configparser
from astroquery.utils.tap.core import TapPlus
from astroquery.casda import Casda
import concurrent.futures
import time
import csv
import pandas as pd
import tarfile
import requests
import copy
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
import tarfile
from typing import BinaryIO

def process_data(credentials:str, input_csv:str, processed_catalogue:str, timeout_seconds:int, project_code:str):
    """
    Processes an input catalogue of unprocessed sources to retrieve relevant data, and saves the processed details to a CSV file 'hipass_ms_file_details.csv' in the working directory.

    Parameters
    ----------
    credentials: Path to the CASDA credentials file.
    input_csv: Path to the input CSV file with source names.
    processed_catalogue: Path to the catalogue of already processed sources.
    timeout_seconds: Timeout setting in seconds for download operations.
    project_code: Code of the project. 

    Returns
    -------
    None
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

        # For every row i.e. HIPASS source 
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
            # Print sbid_visibility_dict
            print("sbid_visibility_dict:")
            print(sbid_visibility_dict)
            
            # Print sbid_evaluation_dict
            print("sbid_evaluation_dict:")
            print(sbid_evaluation_dict)

            # Creating a new vis, eval dict based on the above two dictionaries 
            vis_eval_dict = {sbid_evaluation_dict[key]: value for key, value in sbid_visibility_dict.items()}

            # Print vis_eval_dict
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

            # Print updated_vis_eval_dict
            print("updated_vis_eval_dict:")
            print(updated_vis_eval_dict)

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

            # Dictionary to keep track of duplicate counts for each file
            filename_counts = {}  
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
                    # Keep the original filename on the first occurrence
                    new_filename = file_no_tar  
            
                print(f"File {new_filename} added to i/p for pipeline part B")
                output_data.append([new_filename, f"{ra_h}: {ra_m}: {ra_s:.2f}", f"{dec_d}: {dec_m}: {dec_s:.2f}", vsys])
            
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
    output_df.to_csv(output_csv, index=False, header=True)
    print(f"Output saved to {output_csv}")

# Updated read_and_process which also includes the evaluation file locations
def read_and_process_csv(filename: str) -> list:
    """
    Reads a CSV file and processes its contents, returning a list of dictionaries.

    Parameters
    ----------
    filename: The name of the CSV file to be read.

    Returns
    -------
        A list of dictionaries representing each processed row of the CSV file.
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
                'Cimager.write.weightsimage': 'true',
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

def parset_mixing(static_parset: dict, dynamic_parset: list, prefix: str="") -> bytes
    """
    Update parset with dict values.

    Parameters
    ----------
    static_parset: Standard parset dictionary
    dynamic_parset: List of dictionaries containing key-value pairs to update parset.
    prefix: Prefix to filter which keys should be updated.

    Returns
    -------
        Binary encoded combined parset. 
    """

    for item in dynamic_parset:
        for key, value in item.items():
            if prefix:
                # Update only if key starts with prefix
                if key.startswith(prefix):
                    if key in static_parset:
                        static_parset[key]["value"] = value
                    else:
                        static_parset[key] = {"value": value, "type": "string", "description": ""}
            else:
                # Update all keys if no prefix is provided
                if key in static_parset:
                    static_parset[key]["value"] = value
                else:
                    static_parset[key] = {"value": value, "type": "string", "description": ""}

    serialp = "\n".join(f"{x}={y['value']}" for x, y in static_parset.items())

    return serialp.encode("utf-8")

# HIPASS query with filename pattern 
HIPASS_QUERY_FILENAME = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename LIKE '$filename%'"
)

# TAP Query function
def tap_query(filename:str)->Table:
    """
    Queries the CASDA TAP service for a given filename.
    
    Parameters
    ----------
    filename: The name of the file to query.
    
    Returns
    -------
        Table with query result (files to download).
    """ 
    
    query = HIPASS_QUERY_FILENAME.replace("$filename", filename)
    # print(f"TAP Query: {query}")

    casdatap = TapPlus(url="https://casda.csiro.au/casda_vo_tools/tap", verbose=False)
    job = casdatap.launch_job_async(query)
    res = job.get_results()
    # print(f"Query result: {res}")
    return res

# Code to download files from casda 
def download_file(url:str, check_exists:bool, output:str, timeout:int, buffer=4194304)->str:
    """
    Downloads a file from the specified URL to the given output directory. 
    If a file with the same name already exists, it increments a counter in 
    the filename to avoid overwriting.

    Parameters
    ----------
    url: URL of the file to download.
    check_exists: If True, checks if the file already exists in the output directory and has the same size; skips download if so.
    output: Path to the directory where the file will be saved.
    timeout: Maximum time in seconds to wait for a server response.
    buffer: Buffer size for reading data in chunks during download (default is 4MB).

    Returns
    -------
        The path of the downloaded file.
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
def untar_file(tar_file:str, output_dir='.'):
    """
    Extracts a tar file (.tar, .tar.gz, .tar.bz2) to the specified output directory.

    Parameters
    ----------
    tar_file: Path to the tar file to extract.
    output_dir: Directory where the contents will be extracted. Defaults to the current directory.

    Returns
    -------
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

def degrees_to_hms(degrees:float)->tuple:
    """
    Convert RA given in degrees to hours-minutes-seconds.

    Parameters
    ----------
    degrees: The RA angle in degrees to be converted.

    Returns
    -------
        A tuple (h, m, s) where, h (int): hours component of RA, m (int): minutes component of RA and s (float): seconds component of RA.  
    """

    hours = degrees / 15.0  # Convert degrees to hours
    h = int(hours)  # Integer part of hours
    m = int((hours - h) * 60)  # Integer part of minutes
    s = (hours - h - m / 60.0) * 3600  # Seconds

    return h, m, s

def degrees_to_dms(degrees):
    """
    Convert DEC given in degrees to degrees-minutes-seconds.

    Parameters
    ----------
    degrees: The DEC angle in degrees to be converted.

    Returns
    -------
        A tuple (d, m, s) where d (int): degrees component of the angle, m (int): minutes component of the angle and s (float): seconds component of the angle.
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
# Query on Topcat: select RAJ2000, DEJ2000, VSys from "J/AJ/128/16/table2" where HIPASS like 'J1318-21'
URL_2 = 'http://tapvizier.cds.unistra.fr/TAPVizieR/tap' 

# TAP Query function to get the RA, DEC and Vsys values from Vizier table 
def tap_query_RA_DEC_VSYS(filename:str)->Table:
    """
    Executes a TAP query to retrieve Right Ascension (RA), Declination (DEC) and systemic velocity (VSYS) information based on the provided filename.
    
    Parameters
    ----------
    filename: The name of the file, expected to contain 'HIPASS' if applicable.

    Returns
    -------
        The query results in an Astropy Table format.
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

# Test imager 
def imager():
    """
    Generates a unique filename for the imager output with a 'image_N.fits' format, 
    creates the file and prints a confirmation message with the filename created.

    Parameters
    ----------
    None
    
    Returns
    -------
    None
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
    Generates a unique filename for the imcontsub output with a 'image_N.contsub.fits' extension, 
    creates the file and prints a confirmation message with the filename created.

    Parameters
    ----------
    None
    
    Returns
    -------
    None
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
    Generates a unique filename for the imcontsub output with a 'image_N.contsub_holo.fits' extension, 
    creates the file and prints a confirmation message with the filename created.

    Parameters
    ----------
    None
    
    Returns
    -------
    None
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
    both with a '.10arc.final_mosaic.fits' extension, creates each file and prints confirmation messages with the filenames created.

    Parameters
    ----------
    None
    
    Returns
    -------
    None
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

# HIPASS Query with filename pattern 
HIPASS_QUERY_FILENAME = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename LIKE '$filename%'"
)

URL = "https://casda.csiro.au/casda_vo_tools/tap"

# TAP Query function
def tap_query_filename_visibility(filename:str)-> Table:
    """
    Queries the CASDA TAP service for a given filename.
    
    Parameters
    ----------
    filename: The name of the file to query.
    
    Returns
    -------
        The Table with query result (files to download).
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
def tap_query_sbid_evaluation(sbid:int)->Table:
    """
    Queries the CASDA TAP service for a given filename.
    
    Parameters
    ----------
    sbid: The sbid to query.
    
    Returns
    -------
        The astropy table with query result (files to download).
    """ 

    query = HIPASS_QUERY_sbid.replace("$sbid", str(sbid))  # Convert sbid to string
    print(f"TAP Query: {query}")

    casdatap = TapPlus(url=URL, verbose=False)
    job = casdatap.launch_job_async(query)
    res = job.get_results()
    print(f"Query result: {res}")
    return res

# Function to map evaluation files 
def find_evaluation_file(name:str, updated_vis_eval_dict:dict):
    """
    Finds the key in updated_vis_eval_dict that contains the given filename as a substring.
    
    Parameters
    ----------
    name: The filename to search for within the dictionary values.
    updated_vis_eval_dict: A dictionary where keys are identifiers, and values are lists of filenames.
    
    Returns
    -------
        The key corresponding to the list containing the filename, or None if not found.
    """ 

    for key, filenames in updated_vis_eval_dict.items():
        # Check if the name is a substring of any filename
        if any(name in filename for filename in filenames):
            return key
    return None  # Default if no match is found 

# test versions (without implementing download)
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

DID_URL = "https://casda.csiro.au/casda_data_access/metadata/evaluationEncapsulation"
EVAL_URL = "https://data.csiro.au/casda_vo_proxy/vo/datalink/links?ID="

def download_evaluation_files(filename:str, project_code:str, credentials:str):
    """
    Downloads and extracts evaluation files for a given filename and project code from CASDA.

    Parameters
    ----------
    filename: The filename used to query the visibility data.
    project_code: The project code associated with the observations.
    credentials: Path to the credentials file containing CASDA login details.

    Returns
    -------
    None
    """

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

def download_data_ms(credentials:str, input_csv:str, processed_catalogue:str, timeout_seconds:int, project_code:str):
    """
    Downloads and untars the .ms files for a given HIPASS source. 
    
    Parameters
    ----------
    credentials: Path to the CASDA credentials file.
    input_csv: Path to the input CSV file with source names.
    processed_catalogue: Path to the catalogue of already processed sources.
    timeout_seconds: Timeout setting in seconds for download operations.
    project_code: Code of the project. 

    Returns
    -------
    None
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

        # For every row i.e. HIPASS source 
        for row in csv_reader:
            name = row['Name']

            # Check if the source has already been processed
            if name in processed_sources:
                print(f"{name} already processed")
                continue  # Skip to the next row if the source is processed

            print(f"Querying for: {name}")

            # Get filenames from the query
            res = tap_query(name)
            url_list = casda.stage_data(res, verbose=True)
            print(f"url_list: {url_list}")
            
            # Download files concurrently in the current working directory
            # Empty list to store downloaded filenames 
            file_list = []
            
            # ThreadPoolExecuter created with a maximum of N threads, meaning upto N file downloads can happen simultaneously
            # Tested max_worker values: 4, 6, 12, 16, 32, 64; keep it at max_workers=10
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                
                # List of futures, where each future represents a task to be submitted to the executor 
                futures = [
                    executor.submit(download_file, url=url, check_exists=True, output='.', timeout=timeout_seconds)
                    for url in url_list if not url.endswith('checksum')
                ]

                # For each completed future, save the file-name to file_list
                for future in concurrent.futures.as_completed(futures):
                    file_list.append(future.result())

            # Untar files in the current working directory
            print(f"Untarring files for: {name}")
            for file in file_list:
                if file.endswith('.tar') and tarfile.is_tarfile(file):
                    untar_file(file, '.')

    print(f".ms files downloaded")

def download_data_eval(credentials:str, input_csv:str, processed_catalogue:str, timeout_seconds:int, project_code:str):
    """
    Downloads and untars the evaluation files for a given HIPASS source. 

    Parameters
    ----------
    credentials: Path to the CASDA credentials file.
    input_csv: Path to the input CSV file with source names.
    processed_catalogue: Path to the catalogue of already processed sources.
    timeout_seconds: Timeout setting in seconds for download operations.
    project_code: Code of the project. 

    Returns
    -------
    None
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

        # For every row i.e. HIPASS source 
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

            # Step 3: Downloading the required evaluation files

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

    print(f"Evaluation files downloaded!")