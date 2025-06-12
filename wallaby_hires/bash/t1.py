#!/usr/bin/env python
from mpi4py import MPI
import numpy
import sys
import os
from pathlib import Path
fdir = Path(__file__).parent
comm = MPI.COMM_SELF.Spawn(sys.executable,
                           args=[f'{fdir}/t2.py'],
                           maxprocs=4)

N = numpy.array(100, 'i')
comm.Bcast([N, MPI.INT], root=MPI.ROOT)
PI = numpy.array(0.0, 'd')
comm.Reduce(None, [PI, MPI.DOUBLE],
            op=MPI.SUM, root=MPI.ROOT)
print(PI)

comm.Disconnect()
