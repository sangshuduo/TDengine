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
        self.rowsPerTable = 20
        self.startTime = 1520000010000
        self.rows = 200

        tdDnodes.stop(1)
        tdDnodes.deploy(1)
        tdDnodes.start(1)

        tdSql.execute('reset query cache')
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db rows %d tables 5' % (self.rows))
        tdSql.execute('use db')

        tdLog.info("================= step1")
        tdLog.info("create %d table" % self.ntables)
        for tid in range(1, self.ntables + 1):
            tdSql.execute('create table tb%d (ts timestamp, i int)' % tid)
        tdLog.info(
            "More than 10 rows less than %d rows will go to data file" %
            self.rows)

        tdLog.info("================= step2")
        tdLog.info(
            "import %d data into each %d tables" %
            (self.rowsPerTable, self.ntables))
        for tid in range(1, self.ntables + 1):
            startTime = self.startTime
            sqlcmd = ['import into tb%d values' % tid]
            for rid in range(1, self.rowsPerTable + 1):
                sqlcmd.append(
                    '(%ld+%dd, %d)' %
                    (startTime + rid, rid * 2, rid))
            tdSql.execute(" ".join(sqlcmd))

        tdLog.info("================= step3")
        tdSql.query('select * from tb1')
        tdSql.checkRows(self.rowsPerTable)

        tdLog.info("================= step4")
        tdDnodes.stop(1)
        tdLog.sleep(5)
        tdDnodes.start(1)

        tdLog.info("================= step5")
        tdLog.info("import 1 data before")
        startTime = self.startTime - 1
        for tid in range(1, self.ntables + 1):
            rid = 1
            tdSql.execute(
                'import into tb%d values(%ld+%dd, %d)' %
                (tid, startTime + rid, rid * 2, rid))

        tdLog.info("================= step6")
        tdSql.query('select * from tb1')
        tdSql.checkRows(self.rowsPerTable + 1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
