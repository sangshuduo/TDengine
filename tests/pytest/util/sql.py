﻿###################################################################
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
import os
import time
import datetime
import inspect
from util.log import *
import numpy as np


class TDSql:
    def __init__(self):
        self.queryRows = 0
        self.queryCols = 0
        self.affectedRows = 0

    def init(self, cursor, log=True):
        self.cursor = cursor

        if (log):
            frame = inspect.stack()[1]
            callerModule = inspect.getmodule(frame[0])
            callerFilename = callerModule.__file__
            self.cursor.log(callerFilename + ".sql")

    def close(self):
        self.cursor.close()

    def prepare(self):
        tdLog.info("prepare database:db")
        s = 'reset query cache'
        print(s)
        self.cursor.execute(s)
        s = 'drop database if exists db'
        print(s)
        self.cursor.execute(s)
        s = 'create database db'
        print(s)
        self.cursor.execute(s)
        s = 'use db'
        print(s)
        self.cursor.execute(s)

    def error(self, sql):
        expectErrNotOccured = True
        try:
            self.cursor.execute(sql)
        except BaseException:
            expectErrNotOccured = False
        if expectErrNotOccured:
            frame = inspect.stack()[1]
            callerModule = inspect.getmodule(frame[0])
            callerFilename = callerModule.__file__
            tdLog.exit(
                "%s failed: sql:%s, expect error not occured" %
                (callerFilename, sql))
        else:
            self.queryRows = 0
            self.queryCols = 0
            self.queryResult = None
            tdLog.info("sql:%s, expect error occured" % (sql))

    def query(self, sql):
        self.sql = sql
        print(sql)
        self.cursor.execute(sql)
        self.queryResult = self.cursor.fetchall()
        self.queryRows = len(self.queryResult)
        self.queryCols = len(self.cursor.description)
        # if self.queryRows == 1 and self.queryCols == 1:
        #	tdLog.info("sql:%s, rows:%d cols:%d data:%s" % (self.sql, self.queryRows, self.queryCols, self.queryResult[0][0]))
        # else:
        #	tdLog.info("sql:%s, rows:%d cols:%d" % (self.sql, self.queryRows, self.queryCols))
        return self.queryRows

    def checkRows(self, expectRows):
        if self.queryRows != expectRows:
            frame = inspect.stack()[1]
            callerModule = inspect.getmodule(frame[0])
            callerFilename = callerModule.__file__
            tdLog.exit(
                "%s failed: sql:%s, queryRows:%d != expect:%d" %
                (callerFilename, self.sql, self.queryRows, expectRows))
        tdLog.info("sql:%s, queryRows:%d == expect:%d" %
                   (self.sql, self.queryRows, expectRows))

    def checkDataType(self, row, col, dataType):
        frame = inspect.stack()[1]
        callerModule = inspect.getmodule(frame[0])
        callerFilename = callerModule.__file__

        if row < 0:
            tdLog.exit(
                "%s failed: sql:%s, row:%d is smaller than zero" %
                (callerFilename, self.sql, row))
        if col < 0:
            tdLog.exit(
                "%s failed: sql:%s, col:%d is smaller than zero" %
                (callerFilename, self.sql, col))
        if row > self.queryRows:
            tdLog.exit(
                "%s failed: sql:%s, row:%d is larger than queryRows:%d" %
                (callerFilename, self.sql, row, self.queryRows))
        if col > self.queryCols:
            tdLog.exit(
                "%s failed: sql:%s, col:%d is larger than queryCols:%d" %
                (callerFilename, self.sql, col, self.queryCols))

        return self.cursor.istype(col, dataType)

    def checkData(self, row, col, data):
        frame = inspect.stack()[1]
        callerModule = inspect.getmodule(frame[0])
        callerFilename = callerModule.__file__

        if row < 0:
            tdLog.exit(
                "%s failed: sql:%s, row:%d is smaller than zero" %
                (callerFilename, self.sql, row))
        if col < 0:
            tdLog.exit(
                "%s failed: sql:%s, col:%d is smaller than zero" %
                (callerFilename, self.sql, col))
        if row > self.queryRows:
            tdLog.exit(
                "%s failed: sql:%s, row:%d is larger than queryRows:%d" %
                (callerFilename, self.sql, row, self.queryRows))
        if col > self.queryCols:
            tdLog.exit(
                "%s failed: sql:%s, col:%d is larger than queryCols:%d" %
                (callerFilename, self.sql, col, self.queryCols))
        if self.queryResult[row][col] != data:
            tdLog.exit("%s failed: sql:%s row:%d col:%d data:%s != expect:%s" % (
                callerFilename, self.sql, row, col, self.queryResult[row][col], data))

        if data is None:
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (self.sql, row, col, self.queryResult[row][col], data))
        elif isinstance(data, str):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (self.sql, row, col, self.queryResult[row][col], data))
        elif isinstance(data, datetime.date):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (self.sql, row, col, self.queryResult[row][col], data))
        else:
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%d" %
                       (self.sql, row, col, self.queryResult[row][col], data))

    def getData(self, row, col):
        frame = inspect.stack()[1]
        callerModule = inspect.getmodule(frame[0])
        callerFilename = callerModule.__file__

        if row < 0:
            tdLog.exit(
                "%s failed: sql:%s, row:%d is smaller than zero" %
                (callerFilename, self.sql, row))
        if col < 0:
            tdLog.exit(
                "%s failed: sql:%s, col:%d is smaller than zero" %
                (callerFilename, self.sql, col))
        if row > self.queryRows:
            tdLog.exit(
                "%s failed: sql:%s, row:%d is larger than queryRows:%d" %
                (callerFilename, self.sql, row, self.queryRows))
        if col > self.queryCols:
            tdLog.exit(
                "%s failed: sql:%s, col:%d is larger than queryCols:%d" %
                (callerFilename, self.sql, col, self.queryCols))
        return self.queryResult[row][col]

    def executeTimes(self, sql, times):
        for i in range(times):
            try:
                return self.cursor.execute(sql)
            except BaseException:
                time.sleep(1)
                continue

    def execute(self, sql):
        self.sql = sql
        print(sql)
        self.affectedRows = self.cursor.execute(sql)
        return self.affectedRows

    def checkAffectedRows(self, expectAffectedRows):
        if self.affectedRows != expectAffectedRows:
            frame = inspect.stack()[1]
            callerModule = inspect.getmodule(frame[0])
            callerFilename = callerModule.__file__

            tdLog.exit(
                "%s failed: sql:%s, affectedRows:%d != expect:%d" %
                (callerFilename, self.sql, self.affectedRows, expectAffectedRows))
        tdLog.info("sql:%s, affectedRows:%d == expect:%d" %
                   (self.sql, self.affectedRows, expectAffectedRows))

    def checkColumnSorted(self, col, order):
        frame = inspect.stack()[1]
        callerModule = inspect.getmodule(frame[0])
        callerFilename = callerModule.__file__

        if col < 0:
            tdLog.exit(
                "%s failed: sql:%s, col:%d is smaller than zero" %
                (callerFilename, self.sql, col))
        if col > self.queryCols:
            tdLog.exit(
                "%s failed: sql:%s, col:%d is larger than queryCols:%d" %
                (callerFilename, self.sql, col, self.queryCols))

        matrix = np.array(self.queryResult)
        list = matrix[:, 0]

        if order == "" or order.upper() == "ASC":
            if all(sorted(list) == list):
                tdLog.info(
                    "sql:%s, column :%d is sorted in accending order as expected" %
                    (self.sql, col))
            else:
                tdLog.exit(
                    "%s failed: sql:%s, col:%d is not sorted in accesnind order" %
                    (callerFilename, self.sql, col))
        elif order.upper() == "DESC":
            if all(sorted(list, reverse=True) == list):
                tdLog.info(
                    "sql:%s, column :%d is sorted in decending order as expected" %
                    (self.sql, col))
            else:
                tdLog.exit(
                    "%s failed: sql:%s, col:%d is not sorted in decending order" %
                    (callerFilename, self.sql, col))
        else:
            tdLog.exit(
                "%s failed: sql:%s, the order provided for col:%d is not correct" %
                (callerFilename, self.sql, col))


tdSql = TDSql()
