# wallaby-hires

## WALLABY hires pipeline implemented as a DALiuGE workflow
- The existing test WALLABY hires pipeline was a simple, manually invoked script that was not under version control. It mostly produced configuration files for ASKAPsoft and SLURM. 
- The new pipeline is implemented as a DALiuGE workflow, which is kept under version control on GitHub along with the required additional software components. The workflow and the individual components are configurable using the DALiuGE EAGLE graphical workflow editor, and individual workflow instances (sessions) can be submitted to multiple processing platforms, including a local laptop, the ICRAR Hyades cluster and Setonix.
- The workflow includes components to download the required data from CASDA, prepare the ASKAP configuration files (parameter files), launch the imager, continuum subtraction and primary beam correction for each of the beams and footprints and then run the final mosaicing to combine the individual image cubes into a single output cube and the associated weight cube and upload those to Acacia.
- The final upload location of the data products can be configured, depending on operational needs.
- The main ASKAPsoft components are launched as Docker or Singularity containers, which are provided by the ASKAP software team or Pawsey.




## Installation

There are multiple options for the installation, depending on how you intend to run the DALiuGE engine, directly in a virtual environment (host) or inside a docker container. You can also install it either from PyPI (the latest released version).

## Install it from PyPI

### Engine in a virtual environment
```bash
pip install wallaby_hires
```
This will only work after releasing the project to PyPi.
### Engine in Docker container
```bash
docker exec -t daliuge-engine bash -c 'pip install --prefix=$DLG_ROOT/code wallaby_hires'
```
