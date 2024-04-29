from typing import BinaryIO
from astropy import constants
from astropy import units as u


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

def velocity2channel(velocity:float, low_freq:float=1404.354, high_freq:float=1408.979, nchan:int=250) -> int:
    """
    Calculates the channel number from a redshift velocity. Default values are for
    ASKAP WALLABY observations.

    Parameters:
    -----------
    low_freq: lower frequency limit of the data
    high_freq: upper frequency limit of the data
    nchan: number of channels between low_freq and high_freq

    Returns:
    --------
    The channel containing the velocity
    """
    velocity = velocity * u.km/u.s
    h1_rest_freq = 1420.405752
    chan_width = (high_freq - low_freq) / nchan
    vel_width =  chan_width / h1_rest_freq * constants.codata.c.to(u.km/u.s)
    low_vel = (h1_rest_freq - high_freq) / h1_rest_freq * constants.codata.c.to(u.km/u.s)
    high_vel = (h1_rest_freq - low_freq) / h1_rest_freq * constants.codata.c.to(u.km/u.s)
    if velocity > high_vel or velocity < low_vel:
        print(f"Provided velocity not in range [{low_vel:+.3f}:{high_vel:+.3f}]")
        return -1
    return(int(((high_vel - velocity) / vel_width).value))


