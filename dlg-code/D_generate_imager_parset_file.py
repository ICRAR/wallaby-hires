def generate_imager_parset(uvdata, source_name, fp, RA_string, Dec_string, beam_no):
    """
    Generates an imager parset file for a given UV data and source information and returns the content as a string.

    Parameters:
    - uvdata (str): The UV data string.
    - source_name (str): The name of the source.
    - fp (str): The footprint information.
    - RA_string (str): The Right Ascension string.
    - Dec_string (str): The Declination string.
    - beam_no (str): The beam number.

    Returns:
    - str: The content of the generated imager parset file.
    """
    parset_content = f"""
    Cimager.dataset = {uvdata}
    Cimager.imagetype = fits
    Cimager.MaxUV = 7000
    Cimager.MinUV = 12
    Cimager.Images.Names = [image.i.{source_name}.{fp}.B{beam_no}]
    Cimager.Images.shape = [384, 384]
    Cimager.Images.cellsize = [2arcsec, 2arcsec]
    Cimager.Images.direction = [{RA_string},{Dec_string}, J2000]
    Cimager.Images.restFrequency = HI
    Cimager.nchanpercore = 10
    Cimager.usetmpfs = false
    Cimager.tmpfs = /dev/shm
    Cimager.freqframe = bary
    Cimager.solverpercore = true
    Cimager.nuvwmachines = 1
    Cimager.nwriters = 25
    Cimager.singleoutputfile = true
    Cimager.Channels = [250,0]
    Cimager.gridder.snapshotimaging = false
    Cimager.gridder.snapshotimaging.wtolerance = 2600
    Cimager.gridder.snapshotimaging.longtrack = true
    Cimager.gridder.snapshotimaging.clipping = 0.01
    Cimager.gridder = WProject
    Cimager.gridder.WProject.wmax = 35000
    Cimager.gridder.WProject.nwplanes = 257
    Cimager.gridder.WProject.oversample = 4
    Cimager.gridder.WProject.maxsupport = 1024
    Cimager.gridder.WProject.variablesupport = true
    Cimager.gridder.WProject.offsetsupport = true
    Cimager.gridder.WProject.sharecf = true
    Cimager.solver = Clean
    Cimager.solver.Clean.algorithm = BasisfunctionMFS
    Cimager.solver.Clean.niter = 800
    Cimager.solver.Clean.gain = 0.2
    Cimager.solver.Clean.scales = [0,10,30,60]
    Cimager.solver.Clean.solutiontype = MAXBASE
    Cimager.solver.Clean.verbose = False
    Cimager.solver.Clean.tolerance = 0.01
    Cimager.solver.Clean.weightcutoff = zero
    Cimager.solver.Clean.weightcutoff.clean = false
    Cimager.solver.Clean.decoupled = true
    Cimager.solver.Clean.psfwidth = 256
    Cimager.solver.Clean.logevery = 50
    Cimager.solver.Clean.detectdivergence = true
    Cimager.threshold.minorcycle = [45%, 3.5mJy, 0.5mJy]
    Cimager.threshold.majorcycle = 0.5mJy
    Cimager.ncycles = 3
    Cimager.Images.writeAtMajorCycle = false
    Cimager.preconditioner.Names = [Wiener,GaussianTaper]
    Cimager.preconditioner.GaussianTaper = [12arcsec, 12arcsec, 0deg]
    Cimager.preconditioner.GaussianTaper.isPsfSize = true
    Cimager.preconditioner.GaussianTaper.tolerance = 0.005
    Cimager.preconditioner.preservecf = true
    Cimager.preconditioner.Wiener.robustness = 0.5
    Cimager.restore = true
    Cimager.restore.beam = fit
    Cimager.restore.beam.cutoff = 0.5
    Cimager.restore.beamReference = mid
    """

    return parset_content