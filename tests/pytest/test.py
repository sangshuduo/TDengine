#!/usr/bin/python
###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################
# install pip
# pip install src/connector/python/linux/python2/

# -*- coding: utf-8 -*-
import sys
import getopt
import subprocess
from distutils.log import warn as printf

from util.log import *
from util.dnodes import *
from util.cases import *

import taos

# add testcase here:
from insert.basic import *

if __name__ == "__main__":
    fileName = "all"
    deployPath = ""
    masterIp = ""
    testCluster = False
    valgrind = 0
    stop = 0
    opts, args = getopt.gnu_getopt(sys.argv[1:], 'f:p:m:scgh', [
                               'file=', 'path=', 'master', 'stop', 'cluster', 'valgrind', 'help'])
    for key, value in opts:
        if key in ['-h', '--help']:
            tdLog.printNoPrefix(
                'A collection of test cases written using Python')
            tdLog.printNoPrefix('-f Name of test case file written by Python')
            tdLog.printNoPrefix('-p Deploy Path for Simulator')
            tdLog.printNoPrefix('-m Master Ip for Simulator')
            tdLog.printNoPrefix('-c Test Cluster Flag')
            tdLog.printNoPrefix('-s stop All dnodes')
            sys.exit(0)

        if key in ['-f', '--file']:
            fileName = value

        if key in ['-p', '--path']:
            deployPath = value

        if key in ['-m', '--master']:
            masterIp = value

        if key in ['-c', '--cluster']:
            testCluster = True

        if key in ['-g', '--valgrind']:
            valgrind = 1

        if key in ['-s', '--stop']:
            stop = 1

    if (stop != 0):
        if (valgrind == 0):
            to_be_killed = "taosd"
        else:
            to_be_killed = "valgrind"

        kill_cmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}' | xargs kill -HUP " % to_be_killed
        os.system(kill_cmd)
        time.sleep(1)

        ps_cmd = "ps -ef|grep -w %s| grep -v grep | awk '{print $2}'" % to_be_killed
        process_id = subprocess.check_output(ps_cmd, shell=True)

        while( process_id ):
            os.system(kill_cmd)
            time.sleep(1)
            process_id = subprocess.check_output(ps_cmd, shell=True)

        if (valgrind == 1):
            cmd = "tail -1 valgrind-out.txt"
            tdLog.notice("%s" % subprocess.check_output(cmd, shell=True))

        tdLog.exit('stop All dnodes')

    if masterIp == "":
        tdDnodes.init(deployPath)
        tdDnodes.setValgrind(valgrind)

        if testCluster:
            tdLog.notice("Procedures for testing cluster")
            if fileName == "all":
                tdCases.runAllCluster()
            else:
                tdCases.runOneCluster(fileName)
        else:
            tdLog.notice("Procedures for testing self-deployment")
            tdDnodes.stopAll()
            tdDnodes.deploy(1)
            tdDnodes.start(1)
            conn = taos.connect(
                host='192.168.0.1',
                config=tdDnodes.getSimCfgPath())
            if fileName == "all":
                tdCases.runAllLinux(conn)
            else:
                tdCases.runOneLinux(conn, fileName)
            conn.close()
    else:
        tdLog.notice("Procedures for tdengine deployed in %s" % (masterIp))
        cfgPath = "../../build/test/cfg"   # was: tdDnodes.getSimCfgPath()
        conn = taos.connect(host=masterIp, config=cfgPath)
        if fileName == "all":
            tdCases.runAllWindows(conn)
        else:
            tdCases.runOneWindows(conn, fileName)
        conn.close()
