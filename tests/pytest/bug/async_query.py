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

# -*- coding: utf-8 -*-

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
        tdDnodes.start(1)

        self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
        tdSql.init(self.conn.cursor())

    def run(self):

        tables = 1
        insertBatchs = 100
        startTs = 1530374400000000
        rowsPerBatch = 1000

        tdLog.info("================= step1")
        tdSql.execute('create database db precision us')
        tdSql.execute('use db')
        tdSql.execute(
            'create table st (ts timestamp, value double) tags(a int, b binary(60))')
        for i in range(0, tables):
            tdSql.execute(
                "create table if not exists db.t%d using db.st tags(%d, '%d')" %
                (i, i, i))

        tdLog.info(
            "insert %d batchs(%d rows per batch) per table" %
            (insertBatchs, rowsPerBatch))
        for t in range(0, tables):
            tdLog.info("insert table:db.t%d" % (t))
            for b in range(0, insertBatchs):
                sql = "insert into db.t%d values " % (t)
                for r in range(0, rowsPerBatch):
                    val = b * rowsPerBatch + r
                    rowsTs = startTs + 60000 * val
                    sql += "(%d,%d)" % (rowsTs, val)
                tdSql.execute(sql)

        tdLog.info("================= step2")
        tdLog.info("query %d tables" % (tables))
        insertRows = insertBatchs * rowsPerBatch
        for t in range(0, tables):
            sql = 'select count(*) from db.t%d' % (t)
            tdSql.query(sql)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, insertRows)

        tdLog.info("================= step3")
        concurrent_num = 3000
        querySeconds = 100
        cmd = "c/async_query %d %d" % (concurrent_num, querySeconds)
        if os.system(cmd) != 0:
            tdLog.exit("failed to run %s" % (cmd))

        tdLog.info("================= step4")
        tdLog.sleep(10)
        sql = 'select count(*) from db.t%d' % (0)
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, insertRows)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addCluster(__file__, TDTestCase())
