# 1) Query: 

# Query that worked: (1 beam)
SELECT * FROM ivoa.obscore 
WHERE filename = 'HIPASSJ1318-21_A_beam10_10arc_split.ms.tar'

# Query that worked (6 beams)
SELECT * FROM ivoa.obscore 
WHERE filename LIKE 'HIPASSJ1318-21_%_10arc_split.ms.tar'

HIPASS_QUERY = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename LIKE 'HIPASSJ1318-21_%_10arc_split.ms.tar'"
)

HIPASS_QUERY = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename = 'HIPASSJ1318-21_A_beam10_10arc_split.ms.tar'"
)

# HIPASS Query taking into account the filename 
HIPASS_QUERY = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename LIKE ($filename)"
)

# HIPASS query taking into account the filename
HIPASS_QUERY_TEMPLATE = (
    "SELECT * FROM ivoa.obscore WHERE "
    "filename LIKE '%{}%'"
)