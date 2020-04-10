###################################################################
#       Copyright (c) 2016 by TAOS Technologies, Inc.
#             All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import os
import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self):
        tdLog.debug("start to execute %s" % __file__)
        tdLog.info("prepare cluster")
        tdDnodes.stopAll()
        tdDnodes.deploy(1)
        tdDnodes.cfg(1, "numOfMPeers", "1")
        tdDnodes.cfg(1, "tables", "4")
        tdDnodes.start(1)

        self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
        tdSql.init(self.conn.cursor())
        tdSql.execute('reset query cache')

    def run(self):
        self.ntables = 20
        self.startTime = 1520000010000

        tdLog.info("================= step1")
        tdLog.info(
            "insert 10 records into %d tables in single dnode" %
            self.ntables)
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db')
        tdLog.sleep(5)
        tdSql.execute('use db')
        for tid in range(1, self.ntables + 1):
            tdSql.execute('create table tb%d(ts timestamp, i int)' % tid)
        tdLog.sleep(5)
        for tid in range(1, self.ntables + 1):
            startTime = self.startTime
            sqlcmd = ['insert into tb%d values' % tid]
            for rid in range(1, 11):
                sqlcmd.append('(%ld, %d)' % (startTime + rid, rid))
            tdSql.execute(" ".join(sqlcmd))
        self.startTime += 10
        tdSql.query('select * from tb1')
        tdSql.checkRows(10)

        tdLog.info("================= step2")
        tdLog.info("dnode 2 join the cluster")
        tdSql.execute('create dnode 192.168.0.2')
        tdDnodes.deploy(2)
        tdDnodes.cfg(2, "numOfMPeers", "1")
        tdDnodes.cfg(2, "tables", "4")
        tdDnodes.start(2)
        tdSql.query('show dnodes')
        tdLog.info("%s:%s" % (tdSql.getData(0, 0), tdSql.getData(0, 5)))
        tdLog.info("%s:%s" % (tdSql.getData(1, 0), tdSql.getData(1, 5)))
        while(True):
            tdLog.sleep(20)
            tdSql.query('show dnodes')
            stateRes1 = (tdSql.getData(0, 5) == "balanced")
            stateRes2 = (tdSql.getData(1, 5) == "balanced")
            if (stateRes1 and stateRes2):
                break

        tdLog.info("================= step3")
        tdLog.info("alter database replica to 2")
        tdSql.execute('alter database db replica 2')
        tdSql.query('show dnodes')
        tdLog.info("%s:%s" % (tdSql.getData(0, 0), tdSql.getData(0, 5)))
        tdLog.info("%s:%s" % (tdSql.getData(1, 0), tdSql.getData(1, 5)))
        while(True):
            tdLog.sleep(20)
            tdSql.query('show dnodes')
            stateRes1 = (tdSql.getData(0, 5) == "balanced")
            stateRes2 = (tdSql.getData(1, 5) == "balanced")
            if (stateRes1 and stateRes2):
                break

        tdLog.info("================= step4")
        tdLog.info("insert 10 records into %d tables" % self.ntables)
        for tid in range(1, self.ntables + 1):
            startTime = self.startTime
            sqlcmd = ['insert into tb%d values' % tid]
            for rid in range(1, 11):
                sqlcmd.append('(%ld, %d)' % (startTime + rid, rid))
            tdSql.execute(" ".join(sqlcmd))
        self.startTime += 10
        tdSql.query('select * from tb1')
        tdSql.checkRows(20)
        tdLog.sleep(5)

        tdLog.info("================= step5")
        tdLog.info("dnode 3 join the cluster")
        tdSql.execute('create dnode 192.168.0.3')
        tdDnodes.deploy(3)
        tdDnodes.cfg(3, "numOfMPeers", "1")
        tdDnodes.cfg(3, "tables", "4")
        tdDnodes.start(3)
        tdSql.query('show dnodes')
        tdLog.info("%s:%s" % (tdSql.getData(0, 0), tdSql.getData(0, 5)))
        tdLog.info("%s:%s" % (tdSql.getData(1, 0), tdSql.getData(1, 5)))
        tdLog.info("%s:%s" % (tdSql.getData(2, 0), tdSql.getData(2, 5)))
        while(True):
            tdLog.sleep(20)
            tdSql.query('show dnodes')
            stateRes1 = (tdSql.getData(0, 5) == "balanced")
            stateRes2 = (tdSql.getData(1, 5) == "balanced")
            stateRes3 = (tdSql.getData(2, 5) == "balanced")
            if (stateRes1 and stateRes2 and stateRes3):
                break

        tdLog.info("================= step6")
        tdLog.info("alter database replica to 3")
        tdSql.execute('alter database db replica 3')
        tdSql.query('show dnodes')
        tdLog.info("%s:%s" % (tdSql.getData(0, 0), tdSql.getData(0, 5)))
        tdLog.info("%s:%s" % (tdSql.getData(1, 0), tdSql.getData(1, 5)))
        tdLog.info("%s:%s" % (tdSql.getData(2, 0), tdSql.getData(2, 5)))
        while(True):
            tdLog.sleep(20)
            tdSql.query('show dnodes')
            stateRes1 = (tdSql.getData(0, 5) == "balanced")
            stateRes2 = (tdSql.getData(1, 5) == "balanced")
            stateRes3 = (tdSql.getData(2, 5) == "balanced")
            if (stateRes1 and stateRes2 and stateRes3):
                break

        tdLog.info("================= step7")
        tdLog.info("insert 10 records into %d tables" % self.ntables)
        for tid in range(1, self.ntables + 1):
            startTime = self.startTime
            sqlcmd = ['insert into tb%d values' % tid]
            for rid in range(1, 11):
                sqlcmd.append('(%ld, %d)' % (startTime + rid, rid))
            tdSql.execute(" ".join(sqlcmd))
        self.startTime += 10
        tdSql.query('select * from tb1')
        tdSql.checkRows(30)
        tdLog.sleep(5)

        tdLog.info("================= step6")
        tdLog.info("check database replica")
        tdSql.query('show databases')
        tdSql.checkData(0, 4, 3)

    def stop(self):
        tdSql.close()
        self.conn.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addCluster(__file__, TDTestCase())
