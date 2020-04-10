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
import threading
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
        tdSql.execute('reset query cache')
        tdSql.execute('create dnode 192.168.0.2')
        tdDnodes.deploy(2)
        tdDnodes.start(2)

    def Loop(self):

        conn = taos.connect(config=tdDnodes.getSimCfgPath())
        cursor = conn.cursor()
        threadIndex = threading.current_thread().name

        timestamp = 1530374400000
        for i in range(self.insertBatchs):
            val = 10 * i + 2
            sql = "insert into db.t%s values(%d, %d, %d, %d)" % (
                threadIndex, timestamp + 200, val, val, val)
            cursor.execute(sql)

            val = 10 * i + 3
            sql = "insert into db.t%s values(%d, %d, %d, %d)" % (
                threadIndex, timestamp + 300, val, val, val)
            cursor.execute(sql)

            val = 10 * i + 4
            sql = "insert into db.t%s values(%d, %d, %d, %d)" % (
                threadIndex, timestamp + 400, val, val, val)
            cursor.execute(sql)

            val = 10 * i + 5
            sql = "insert into db.t%s values(%d, %d, %d, %d)" % (
                threadIndex, timestamp + 500, val, val, val)
            cursor.execute(sql)

            val = 10 * i + 6
            sql = "insert into db.t%s values(%d, %d, %d, %d)" % (
                threadIndex, timestamp + 600, val, val, val)
            cursor.execute(sql)

            val = 10 * i + 7
            sql = "insert into db.t%s values(%d, %d, %d, %d)" % (
                threadIndex, timestamp + 700, val, val, val)
            cursor.execute(sql)

            val = 10 * i + 8
            sql = "insert into db.t%s values(%d, %d, %d, %d)" % (
                threadIndex, timestamp + 800, val, val, val)
            cursor.execute(sql)

            val = 10 * i + 9
            sql = "insert into db.t%s values(%d, %d, %d, %d)" % (
                threadIndex, timestamp + 900, val, val, val)
            cursor.execute(sql)

            val = 10 * i + 1
            sql = "import into db.t%s values(%d, %d, %d, %d)" % (
                threadIndex, timestamp + 100, val, val, val)
            cursor.execute(sql)

            val = 10 * i + 0
            sql = "import into db.t%s values(%d, %d, %d, %d)" % (
                threadIndex, timestamp + 0, val, val, val)
            cursor.execute(sql)

            timestamp += 1000

        cursor.close()
        conn.close()
        # tdLog.debug('thread %s ended.' % threading.current_thread().name)

    def run(self):

        self.tables = 10
        self.insertBatchs = 1000
        self.replica = 1
        self.rowsPerBatch = 10

        tdLog.info("================= step1")
        tdSql.execute('create database db replica 2')
        tdSql.execute('use db')
        tdSql.execute(
            'create table st (ts timestamp, i int, j float, k double) tags(a int, b binary(60))')
        for i in range(0, self.tables):
            tdSql.execute(
                "create table if not exists db.t%d using db.st tags(%d, '%d')" %
                (i, i, i))
        tdLog.sleep(10)

        tdLog.info("================= step2")
        threads = []
        for t in range(self.tables):
            threadName = "%d" % (t)
            thread = threading.Thread(target=self.Loop, name=threadName)
            thread.start()
            threads.append(thread)

        for t in range(self.tables):
            threads[t].join()

        tdLog.info("================= step3")
        tdLog.info("query %d tables" % (self.tables))
        insertRows = self.insertBatchs * self.rowsPerBatch
        for t in range(0, self.tables):
            sql = 'select count(*) from db.t%d' % (t)
            tdSql.query(sql)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, insertRows)

        tdLog.info("================= step4")
        tdLog.info("query %d tables" % (self.tables))
        insertRows = self.insertBatchs * self.rowsPerBatch
        for t in range(0, self.tables):
            sql = 'select * from db.t%d' % (t)
            tdSql.query(sql)
            tdSql.checkRows(insertRows)

        tdLog.info("================= step5")
        sql = "select * from db.st"
        insertRows = self.insertBatchs * self.rowsPerBatch * self.tables
        tdSql.query(sql)
        tdSql.checkRows(insertRows)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addCluster(__file__, TDTestCase())
