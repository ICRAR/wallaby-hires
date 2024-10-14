How to run it: casda_download_filename.py

1) On laptop: 
. ~/.venv/bin/activate

time python casda_download_filename.py -f "HIPASSJ1318-21" -o "/home/gayatri/Downloads/download_data_casda/HIPASSJ1318-21" -p "WALLABY" -c "casda.ini" --timeout 1800

2) On Hyades: 
time python3 casda_download_filename.py -f "HIPASSJ1318-21" -o "/home/ganiruddha/download_data_casda/HIPASSJ1318-21" -p "WALLABY" -c "casda.ini" --timeout 1800

3) On Setonix:
Before running any of the download scripts: (On Setonix)
### Create a virtual environment
python -m venv myenv

### Activate the virtual environment
source myenv/bin/activate

### Install the required packages (Example)
pip install astropy 

### Command 
time python3 casda_download_filename.py -f "HIPASSJ1318-21" -o "/scratch/pawsey0411/ganiruddha/Setonix/download_data_casda/HIPASSJ1318-21" -p "WALLABY" -c "casda.ini" --timeout 1800

How to run it: casda_download_wallaby-hires.py
1) On Laptop 
time python casda_download_wallaby-hires.py -f "HIPASSJ1318-21_A_beam10_10arc_split.ms.tar" -o "/home/gayatri/Downloads/download_data_casda" -p "WALLABY" -c "casda.ini" --timeout 1800