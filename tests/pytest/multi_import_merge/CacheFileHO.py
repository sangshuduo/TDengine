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

import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        self.ntables = 10
        self.rowsPerTable = 40
        self.startTime = 1520000010000

        tdDnodes.stop(1)
        tdDnodes.deploy(1)
        tdDnodes.start(1)

        tdSql.execute('reset query cache')
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db tables 5')
        tdSql.execute('use db')

        tdLog.info("================= step1")
        tdLog.info("create %d table" % self.ntables)
        for tid in range(1, self.ntables + 1):
            tdSql.execute('create table tb%d (ts timestamp, i int)' % tid)

        tdLog.info("================= step2")
        tdLog.info(
            "import %d data into each %d tables" %
            (self.rowsPerTable, self.ntables))
        for tid in range(1, self.ntables + 1):
            startTime = self.startTime
            sqlcmd = ['import into tb%d values' % tid]
            for rid in range(1, self.rowsPerTable + 1):
                sqlcmd.append('(%ld+%dd, %d)' % (startTime + rid, rid, rid))
            tdSql.execute(" ".join(sqlcmd))

        tdLog.info("================= step3")
        tdSql.query('select * from tb1')
        tdSql.checkRows(self.rowsPerTable)

        tdLog.info("================= step4")
        tdDnodes.stop(1)
        tdLog.sleep(5)
        tdDnodes.start(1)

        tdLog.info("================= step5")
        tdLog.info("import 10 data again")
        for tid in range(1, self.ntables + 1):
            startTime = self.startTime + self.rowsPerTable
            sqlcmd = ['import into tb%d values' % tid]
            for rid in range(1, 11):
                sqlcmd.append('(%ld+%dd, %d)' % (startTime + rid, rid, rid))
            tdSql.execute(" ".join(sqlcmd))

        tdLog.info("================= step6")
        tdSql.query('select * from tb1')
        tdSql.checkRows(self.rowsPerTable + 10)

        tdLog.info("================= step7")
        tdLog.info("import 2 data before with overlap")
        for tid in range(1, self.ntables + 1):
            startTime = self.startTime - 1
            sqlcmd = ['import into tb%d values' % tid]
            for rid in range(1, 3):
                sqlcmd.append('(%ld+1d, %d)' % (startTime + rid, rid))
            tdSql.execute(" ".join(sqlcmd))
        tdSql.checkAffectedRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
