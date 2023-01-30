#!/usr/bin/env python3

__author__    = 'RADICAL-Cybertools Team'
__email__     = 'info@radical-cybertools.org'
__copyright__ = 'Copyright 2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.pilot as rp

N_NODES = 1
N_TASKS = 6

PILOT_DESCRIPTION = {
    'resource' : 'ornl.crusher',
    'project'  : 'CSC449_crusher',
    'nodes'    : N_NODES,
    'runtime'  : 15
}


def main():

    session = rp.Session()
    try:
        pmgr = rp.PilotManager(session=session)
        tmgr = rp.TaskManager(session=session)

        pilot = pmgr.submit_pilots(rp.PilotDescription(PILOT_DESCRIPTION))
        tmgr.add_pilots(pilot)

        tds = []
        for _ in range(N_TASKS):

            tds.append(rp.TaskDescription({
                'ranks'         : 1,
                'cores_per_rank': 10,
                'threading_type': rp.OpenMP,
                'gpus_per_rank' : 1,
                # RP test executable:
                # https://github.com/radical-cybertools/radical.pilot/blob/devel/bin/radical-pilot-hello.sh
                'executable'    : 'radical-pilot-hello.sh',
                'arguments'     : [10]
            }))

        tmgr.submit_tasks(tds)
        tmgr.wait_tasks()
    finally:
        session.close(download=True)


if __name__ == '__main__':

    os.environ['RADICAL_PROFILE'] = 'TRUE'
    # for test purposes
    os.environ['RADICAL_LOG_LVL'] = 'DEBUG'

    main()

