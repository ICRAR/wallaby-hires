import astropy 
from astropy.coordinates import SkyCoord
import astropy.units as u

def process_each_source(source):
    """
    Process a line of data and return a dictionary with extracted parameters.

    Parameters:
    -----------
    source (list): A list representing a line of data.

    Returns:
    -------
    dict: A dictionary containing extracted parameters {name, RA_string, Dec_string, Vsys}.
    """
    # Extract individual parameters
    name = str(source[0]).strip()
    
    # Use SkyCoord for RA and Dec conversion
    coord = SkyCoord(source[1], source[2], unit=(u.hourangle, u.deg))
    RA_string = coord.ra.to_string(unit=u.hour, sep='h', precision=2, pad=True)
    Dec_string = coord.dec.to_string(unit=u.deg, sep='d', precision=1, pad=True)

    Vsys = float(source[3])

    # Create a dictionary for the current source
    source_dict = {'name': name, 'RA_string': RA_string, 'Dec_string': Dec_string, 'Vsys': Vsys}

    # Return the dictionary 
    return source_dict