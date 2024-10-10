#!/usr/bin/env python3

'''
Notes: 
- Reference link: https://github.com/AusSRC/pipeline_components/blob/main/casda_download/casda_download.py
- Comments added from ChatGPT (07/10/24)


'''

# Provides a way of interacting with the operating system
import os

# Allows interaction with the Python interpreter
import sys

# Enables the creation of log messages for tracking events that happen during program execution
import logging

# Provides methods for parsing JSON (JavaScript Object Notation) data and converting Python objects to JSON format
import json

# Provides functions for working with URLs, including opening, reading, and parsing URLs
import urllib

# Enables asynchronous programming allowing for concurrent execution of code without using traditional threading or multiprocessing
import asyncio

# Provides core functionality for astronomy and astrophysics, such as handling astronomical data, units, and time calculations
import astropy

# Provides functionality for working with configuration files (typically .ini format), enabling you to read, modify, and write configuration settings
import configparser

# astroquery is a package for querying astronomical databases
# TapPlus provides an interface for working with Table Access Protocol (TAP) services, which are commonly used to query large astronomical datasets
from astroquery.utils.tap.core import TapPlus

# Part of astroquery, this module specifically deals with querying and retrieving data from the CASDA (CSIRO ASKAP Science Data Archive) database
from astroquery.casda import Casda

# Provides a high-level interface for asynchronously executing code using threads or processes, which allows you to execute tasks concurrently
import concurrent.futures

'''
The logging.basicConfig() function configures the logging system in Python, setting up how log messages will be handled.
stream=sys.stdout: 
- This specifies that the log messages will be sent to sys.stdout (i.e., the standard output, typically your terminal or console).

level=logging.INFO:
- Only log messages with a severity level of INFO and above (i.e., INFO, WARNING, ERROR, CRITICAL) will be processed and shown. 
- Lower severity levels like DEBUG will be ignored.

format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
- This specifies how each log message will be formatted. 
- The format string contains placeholders that are replaced with specific information about the log event.

'''
logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')


# Astropy will not automatically download the IERS data files if they are not present on your system.
astropy.utils.iers.conf.auto_download = False


URL = "https://casda.csiro.au/casda_vo_tools/tap"

WALLABY_QUERY = (
    "SELECT * FROM ivoa.obscore WHERE obs_id IN ($SBIDS) AND "
    "dataproduct_type='cube' AND ("
    "filename LIKE 'weights.i.%.cube.fits' OR "
    "filename LIKE 'image.restored.i.%.cube.contsub.fits')")

WALLABY_MILKYWAY_QUERY = (
    "SELECT * FROM ivoa.obscore WHERE obs_id IN ($SBIDS) "
    "AND dataproduct_type='cube' AND "
    "(filename LIKE 'weights.i.%.cube.MilkyWay.fits' OR filename LIKE 'image.restored.i.%.cube.MilkyWay.contsub.fits')"
)

POSSUM_QUERY = (
    "SELECT * FROM ivoa.obscore WHERE obs_id IN ($SBIDS) AND "
    "dataproduct_type='cube' AND ("
    "filename LIKE 'image.restored.i.%.contcube.conv.fits' OR "
    "filename LIKE 'weights.q.%.contcube.fits' OR "
    "filename LIKE 'image.restored.q.%.contcube.conv.fits' OR "
    "filename LIKE 'image.restored.u.%.contcube.conv.fits')")

EMU_QUERY = (
    "SELECT * FROM ivoa.obscore WHERE obs_id IN ($SBIDS) AND ( "
    "filename LIKE 'image.i.%.cont.taylor.%.restored.conv.fits' OR "
    "filename LIKE 'weights.i.%.cont.taylor%.fits')")

DINGO_QUERY = (
    "SELECT * FROM ivoa.obscore WHERE obs_id IN ($SBIDS) AND "
    "(filename LIKE 'weights.i.%.cube.fits' OR "
    "filename LIKE 'image.restored.i.%.cube.contsub.fits' OR "
    "filename LIKE 'image.i.%.0.restored.conv.fits')")

def parse_args(argv):
    parser = argparse.ArgumentParser()

    # The add_argument method is to define the command-line arguments the script will accept
    # --sbid --output m--project --credentials --manifest 
    parser.add_argument(
        "-s",
        "--sbid",
        type=str,
        required=True,
        action='append',
        nargs='+',
        help="Scheduling block id number.",)

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
        help="ASKAP project name (WALLABY or POSSUM).",)

    parser.add_argument(
        "-c",
        "--credentials",
        type=str,
        required=False,
        help="CASDA credentials config file.",
        default="./casda.ini",)

    parser.add_argument(
        "-m",
        "--manifest",
        type=str,
        required=False,
        help="Manifest Output",)

    parser.add_argument(
        "-t", "--timeout", 
        type=int, 
        required=False, 
        default=3000, 
        help="CASDA download file timeout [seconds]")

    # This method parses the list of command-line arguments passed to the function (argv) and returns an args object that contains the parsed values.
    args = parser.parse_args(argv)
    return args

'''
- The function tap_query(project, sbid) is designed to generate and execute a TAP (Table Access Protocol) query to retrieve data from a database, 
- specifically files related to a given scheduling block ID (sbid) within a specified project (such as "WALLABY", "POSSUM", etc.). 
- The function uses the TapPlus class from the astroquery package to perform the query and return the results as an Astropy table.
'''
def tap_query(project, sbid):
    """Return astropy table with query result (files to download)"""

    ids = [f"'{str(i)}'" for i in sbid[0]]

    if project == "WALLABY":
        logging.info(f"Scheduling block ID: {sbid}")
        query = WALLABY_QUERY.replace("$SBIDS", ",".join(ids))
        query = query.replace("$SURVEY", str(project))
        logging.info(f"TAP Query: {query}")

    elif project == "WALLABY_MILKYWAY":
        logging.info(f"Scheduling block ID: {sbid}")
        query = WALLABY_MILKYWAY_QUERY.replace("$SBIDS", ",".join(ids))
        query = query.replace("$SURVEY", str(project))
        logging.info(f"TAP Query: {query}")

    elif project == "POSSUM":
        logging.info(f"Scheduling block ID: {sbid}")
        query = POSSUM_QUERY.replace("$SBIDS", ",".join(ids))
        query = query.replace("$SURVEY", str(project))
        logging.info(f"TAP Query: {query}")

    elif project == "DINGO":
        logging.info(f"Scheduling block ID: {sbid}")
        query = DINGO_QUERY.replace("$SBIDS", ",".join(ids))
        query = query.replace("$SURVEY", str(project))
        logging.info(f"TAP Query: {query}")

    elif project == "EMU":
        logging.info(f"Scheduling block ID: {sbid}")
        query = EMU_QUERY.replace("$SBIDS", ",".join(ids))
        query = query.replace("$SURVEY", str(project))
        logging.info(f"TAP Query: {query}")
    else:
        raise Exception('Unexpected project name provided.')

    casdatap = TapPlus(url=URL, verbose=False)
    job = casdatap.launch_job_async(query)
    res = job.get_results()
    logging.info(f"Query result: {res}")
    return res

'''
- The download_file function is designed to download a file from a given URL, save it to a specified output directory, and ensure the file is downloaded completely and correctly. 
- The function includes checks for file existence, size validation, and proper error handling. 
'''
def download_file(url, check_exists, output, timeout, buffer=4194304):
    # Large timeout is necessary as the file may need to be stage from tape
    logging.info(f"Requesting: URL: {url} Timeout: {timeout}")

    try:
        os.makedirs(output)
    except:
        pass

    if url is None:
        raise ValueError('URL is empty')

    with urllib.request.urlopen(url, timeout=timeout) as r:
        filename = r.info().get_filename()
        filepath = f"{output}/{filename}"
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

'''
- The provided code defines an asynchronous Python function main that downloads image cubes from CASDA based on observing block IDs provided as command-line arguments
'''
async def main(argv):
    """Downloads image cubes from CASDA matching the observing block IDs
    provided in the arguments.

    """
    args = parse_args(argv)
    res = tap_query(args.project, args.sbid)
    logging.info(res)

    # stage
    parser = configparser.ConfigParser()
    parser.read(args.credentials)
    casda = Casda()
    casda = Casda(parser["CASDA"]["username"], parser["CASDA"]["password"])
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
    argv = sys.argv[1:]
    asyncio.run(main(argv))
