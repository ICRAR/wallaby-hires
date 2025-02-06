# wallaby-hires

## WALLABY "hi-res" imaging pipeline implemented as a DALiuGE graph
- The existing test WALLABY hires pipeline was a simple, manually invoked script that was not under version control. It mostly produced configuration files for ASKAPsoft and SLURM. 
- The new pipeline is implemented as a DALiuGE workflow, which is kept under version control on GitHub along with the required additional software components. The workflow and the individual components are configurable using the DALiuGE EAGLE graphical workflow editor, and individual workflow instances (sessions) can be submitted to multiple processing platforms, including a local laptop, the ICRAR Hyades cluster and Setonix.
- The workflow includes components to download the required data from CASDA, prepare the ASKAP configuration files (parameter files), launch the imager, continuum subtraction and primary beam correction for each of the beams and footprints and then run the final mosaicing to combine the individual image cubes into a single output cube and the associated weight cube and upload those to Acacia.
- The final upload location of the data products can be configured, depending on operational needs.
- The main ASKAPsoft components are launched as Docker or Singularity containers, which are provided by the ASKAP software team or Pawsey.
- In operations, this workflow will be controlled by another long-running workflow, which will poll CASDA for new observations in a configurable cadence (maybe once a day) and trigger the main imaging workflow once new data becomes available.
- Here, two versions of the graph are implemented: test and deployment versions. Both versions were tested and benchmarked on ICRAR's local cluster Hyades and on the Setonix supercomputer at Pawsey. 

### Original workflow of the pipeline
![Alt text](images/wallaby-hires-old-pipeline.png)

### Test and Deployment versions of the DAliuGE graph
- The only difference between the test and deployment versions of the graph is that, in the test version, the imager, imcontsub, linmos, and mosaicking components are replaced with Python functions: imager(), imcontsub(), linmos(), and mosaic().
- The imager() function generates a new FITS file (.fits) named in the format "image_N.fits". Before creating a file, it checks if "image.fits" already exists. If it does, the function increments a counter and appends it to the filename (e.g., "image_2.fits", "image_3.fits", etc.) until an available name is found. It then creates an empty file with the determined filename and prints a confirmation message.
- The imcontsub() and linmos() functions follow a similar approach, generating empty FITS files in the formats "imcontsub_N.fits" and "linmos_N.fits", respectively.
- Finally, the mosaic() function creates two empty FITS files: "image.10arc.final_mosaic.fits" and "weights.10arc.final_mosaic.fits".
- In the deployment version of the graph, the imager, imcontsub, linmos, and mosaicking components are executed within a Docker container using the icrar/yanda_imager:0.4 or csirocass/yandasoft:dev-openmpi2 image.

### Current workflow of the pipeline implemented as a DALiuGE graph
Inputs: 
1. Catalogue: Path to the CSV file containing HIPASS sources to be processed.
2. Processed catalogue: Path to the CSV file with names of already processed sources.
3. Credentials: Path to the CASDA credentials file. 
Processing steps:
1. Processes the input source catalogue: Checks for existing sources, identifies unprocessed sources, retrieves relevant data (RA, DEC, and Vsys), stages files for download, and saves the processed data in a specified CSV format containing the information: source name, RA, DEC, Vsys and evaluation_file_path. 
2. Reads the CSV file: Returns a list of dictionaries from the processed file that contain the dynamic parsets corresponding to all the beams of the given HIPASS source.
3. Combines parameter sets: Merges static and dynamic parameter sets for all beams of each HIPASS source.
4. Inputs for imaging stages: The complete parameter sets are supplied as inputs to the ASKAPsoft components: imager, imcontsub, and linmos.
     - These components are launched inside Docker or Singularity containers on Hyades and Setonix, respectively. 
6. Final step - mosaicking: When all the beams are processed, mosaicking is performed using the output files from the linmos stage. 

### Logical graph

### Physical graph
