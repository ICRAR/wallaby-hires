'''
Start date: 
23/07/24 (noted)
31/07/24 (actual)
End date: 
What: Code to automatically download .ms files from casda to Setonix/laptop. 

Links from Austin: 
https://astroquery.readthedocs.io/en/latest/casda/casda.html
https://github.com/AusSRC/pipeline_components/blob/main/casda_download/casda_download.py

Self links:
1) DALiuGE (Wallaby)
Link: https://docs.google.com/document/d/1P9zukHujRgKNsYbltH9Z0oZnUttMBhXVQUIz_N_mj_o/edit
How to download an observation from CASDA (on Setonix)?

2) Benchmarks
https://docs.google.com/spreadsheets/d/1KDzBG3_VFEl17mp58S_I8pN0V_vJBz9Xt-mx3x0fabM/edit?gid=1846631247#gid=1846631247

3) Total
As of 31/07/24 
- Total sources: 62 
- Total .ms files: 380 (As each source has around 4-12 fp data)
Results: 

Main keyword: HIPASS*_*10arc_split.ms

Test observation to download: 
HIPASSJ1318-21_A_beam10_10arc_split (1 fp)
HIPASSJ1318-21*_*10arc_split (all 6 fp)

HIPASS*_*10arc_split (generic)

Generic link to download: 
https://data.csiro.au/domain/casdaObservation?dataProducts=%5B%7B%22dataProduct%22%3A%22CATALOGUE%22%7D,%7B%22dataProduct%22%3A%22IMAGE_CUBE%22%7D,%7B%22dataProduct%22%3A%22IMAGE_CUBE_ANCILLARY%22%7D,%7B%22dataProduct%22%3A%22SPECTRUM%22%7D,%7B%22dataProduct%22%3A%22MEASUREMENT_SET%22%7D%5D&facets=%5B%5D&showRejected=false&releasedFilter=released&includeCommensalProjects=false&showFacets=true&telescopes=%5B%22ASKAP%22%5D&atcaArray=%5B%5D&searchType=ASKAP&project=AS102%20-%20ASKAP%20Pilot%20Survey%20for%20WALLABY


Specific link to download with the following value prefilled: Filename: "HIPASS*_*10arc_split"
https://data.csiro.au/domain/casdaObservation?dataProducts=%5B%7B%22dataProduct%22%3A%22CATALOGUE%22%7D,%7B%22dataProduct%22%3A%22IMAGE_CUBE%22%7D,%7B%22dataProduct%22%3A%22IMAGE_CUBE_ANCILLARY%22%7D,%7B%22dataProduct%22%3A%22SPECTRUM%22%7D,%7B%22dataProduct%22%3A%22MEASUREMENT_SET%22%7D%5D&facets=%5B%5D&showRejected=false&releasedFilter=released&includeCommensalProjects=false&showFacets=true&telescopes=%5B%22ASKAP%22%5D&atcaArray=%5B%5D&searchType=ASKAP&project=AS102%20-%20ASKAP%20Pilot%20Survey%20for%20WALLABY&filename=HIPASS%2a_%2a10arc_split

'''

