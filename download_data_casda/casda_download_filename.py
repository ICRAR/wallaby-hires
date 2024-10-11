#!/usr/bin/env python3

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

logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')

astropy.utils.iers.conf.auto_download = False

URL = "https://casda.csiro.au/casda_vo_tools/tap"

# HIPASS specific query 
HIPASS_QUERY_SPECIFIC = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename = 'HIPASSJ1318-21_A_beam10_10arc_split.ms.tar'"
)

# HIPASS Query with exact filename pattern 
HIPASS_QUERY_FILENAME_EXACT = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename LIKE '$filename'"
)

# HIPASS Query with filename pattern 
HIPASS_QUERY_FILENAME = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename LIKE '$filename%'"
)

# Defining all the command line arguments 
def parse_args(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-f",
        "--filename",
        type=str,
        required=True,
        help="HIPASS filename",)

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        help="Output directory for downloaded files.",)

    parser.add_argument(
        "-p",
        "--project",
        type=str,
        required=True,
        help="AS102 - ASKAP Pilot Survey for WALLABY.",)

    parser.add_argument(
        "-c",
        "--credentials",
        type=str,
        required=True,
        help="CASDA credentials config file.",
        default="./casda.ini",)
    
    parser.add_argument(
        "-m",
        "--manifest",
        type=str,
        required=False,
        help="Manifest Output",)

    parser.add_argument(
        "-t", 
        "--timeout",
        type=int, 
        required=False, 
        default=3000, help="CASDA download file timeout [seconds]")

    args = parser.parse_args(argv)
    return args

# astropy table with query results 
def tap_query(project, filename):
    """Return astropy table with query result (files to download)"""

    if project == "WALLABY":
        # query = HIPASS_QUERY_SPECIFIC
        query = HIPASS_QUERY_FILENAME.replace("$filename", filename)

        logging.info(f"TAP Query: {query}")

    else:
        raise Exception('Unexpected project name provided.')

    casdatap = TapPlus(url=URL, verbose=False)
    job = casdatap.launch_job_async(query)
    res = job.get_results()
    logging.info(f"Query result: {res}")
    return res

# Downloading files
def download_file(url, check_exists, output, timeout, buffer=4194304):
    # Large timeout is necessary as the file may need to be stage from tape
    
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
            base, ext = os.path.splitext(filename)
            counter = 2
            new_filepath = filepath
            
            # Continue incrementing the filename until a unique one is found
            while os.path.exists(new_filepath):
                new_filename = f"{base}_{counter}{ext}"
                new_filepath = f"{output}/{new_filename}"
                counter += 1
            filepath = new_filepath

        http_size = int(r.info()['Content-Length'])

        if check_exists:
            try:
                file_size = os.path.getsize(filepath)
                if file_size == http_size:
                    logging.info(f"File exists, ignoring: {os.path.basename(filepath)}")
                    # File exists and is same size; do nothing
                    return filepath
            except FileNotFoundError:
                pass

        logging.info(f"Downloading: {filepath} size: {http_size}")
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

        logging.info(f"Download complete: {os.path.basename(filepath)}")

        return filepath

# Main function    
async def main(argv):
    """Downloads visibilities (.ms files) from CASDA matching the filenames provided in the arguments.

    """
    args = parse_args(argv)
    res = tap_query(args.project, args.filename)
    logging.info(res)

    # stage
    parser = configparser.ConfigParser()
    parser.read(args.credentials)

    casda = Casda()
    casda = Casda(parser["CASDA"]["username"], parser["CASDA"]["password"])
    # casda.login(username=parser["CASDA"]["username"], password=parser["CASDA"]["password"])
    # casda.login(username=parser["CASDA"]["username"], store_password=True)    

    # Stage the data (retrieve the list of URLs to download)
    url_list = casda.stage_data(res, verbose=True)
    logging.info(f"CASDA download staged data URLs: {url_list}")

    file_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for url in url_list:
            if url.endswith('checksum'):
                continue
            futures.append(executor.submit(download_file, url=url, check_exists=True, output=args.output, timeout=args.timeout))

        for future in concurrent.futures.as_completed(futures):
            file_list.append(future.result())

    if args.manifest:
        try:
            os.makedirs(os.path.dirname(args.manifest))
        except:
            pass

        with open(args.manifest, "w") as outfile:
            outfile.write(json.dumps(file_list))

if __name__ == "__main__":
    start_time = time.time()  # Record the start time

    argv = sys.argv[1:]
    asyncio.run(main(argv))

    end_time = time.time()  # Record the end time
    elapsed_time = end_time - start_time  # Calculate the elapsed time
    print(f"Script took {elapsed_time:.2f} seconds to run.")