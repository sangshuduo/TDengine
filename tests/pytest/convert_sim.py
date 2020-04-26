import re
import sys
import getopt
from distutils.log import warn as printf


def printBegin():
    printf(
        '''# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()
'''
    )


def printEnd():
    printf(
        '''
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())'''
    )

def printCreateTable(line):
    cmd = line.split(' ', 1)[1].replace("$", "")
    printf("        tdLog.info('%s')" % cmd)
    if "-x step" in line:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        tdSql.error('%s')" % cmd)
    else:
        printf("        tdSql.execute('%s')" % cmd)

def printInsert(line):
    cmd = line.split(' ', 1)[1].replace("$", "")
    printf('        tdLog.info("%s")' % cmd)
    if "-x step" in line:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf('        tdSql.error("%s")' % cmd)
    else:
        printf('        tdSql.execute("%s")' % cmd)

def printSqlError(line):
    cmd = line.split(' ', 1)[1].replace("$", "")
    cmd = re.sub(r' -x step[0-9]?', '', cmd)
    printf('        tdLog.info("%s")' % cmd)
    printf('        tdSql.error("%s")' % cmd)

def printSqlSelect(line):
    cmd = line.split(' ', 1)[1].replace("$", "")
    printf("        tdLog.info('%s')" % cmd)
    if "-x step" in line:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        tdSql.error('%s')" % cmd)
    else:
        printf("        tdSql.query('%s')" % cmd)

def printSqlDrop(line):
    cmd = line.split(' ', 1)[1].replace("$", "")
    printf("        tdLog.info('%s')" % cmd)
    if "-x step" in line:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        tdSql.error('%s')" % cmd)
    else:
        printf("        tdSql.execute('%s')" % cmd)

def printSqlShow(line):
    cmd = line.split(' ', 1)[1].replace("$", "")
    printf("        tdLog.info('%s')" % cmd)
    if "-x step" in line:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        tdSql.error('%s')" % cmd)
    else:
        printf("        tdSql.query('%s')" % cmd)

def printSqlDescribe(line):
    cmd = line.split(' ', 1)[1].replace("$", "")
    printf("        tdLog.info('%s')" % cmd)
    if "-x step" in line:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        tdSql.error('%s')" % cmd)
    else:
        printf("        tdSql.query('%s')" % cmd)

def printIfRows(line):
    expectedRows = line.split(' ')[3]
    printf(
        "        tdLog.info('tdSql.checkRow(%s)')" %
        expectedRows)
    printf("        tdSql.checkRows(%s)" % expectedRows)

def printIfData(line):
    colAndRow = line.split(' ')[1].replace("$data", "")
    checkCol = colAndRow[0:1]
    checkRow = colAndRow[1:2]

    if "@" in line:
        expectedData = re.search('@(.*)@', line).group(1)
        printf(
            '        tdLog.info("tdSql.checkData(%s, %s, %s)")' % \
            (checkCol, checkRow, expectedData))

        printf('        expectedData = datetime.datetime.strptime("%s", '\
            '"%%y-%%m-%%d %%H:%%M:%%S.%%f")'% expectedData)
        printf("        tdSql.checkData(%s, %s, %s)" % \
            (checkCol, checkRow, "expectedData"))
    else:
        expectedData = line.split(' ')[3]
        printf(
            "        tdLog.info('tdSql.checkData(%s, %s, %s)')" % \
            (checkCol, checkRow, expectedData))

        if (expectedData.lower() == "null"):
            printf("        tdSql.checkData(%s, %s, %s)" % \
                (checkCol, checkRow, "None"))
        else:
            printf("        tdSql.checkData(%s, %s, %s)" % \
                (checkCol, checkRow, expectedData))

if __name__ == "__main__":
    origin = False
    i = 0

    opts, args = getopt.gnu_getopt(sys.argv[1:], 'f:ch', [
        'file=', 'comment', 'help'])
    for key, value in opts:
        if key in ['-h', '--help']:
            printf("%s Usage:" % __file__)
            printf('    -h, --help')
            printf(
                '    -f --file filename, Specify a name of TSIM test case file to convert')
            printf('    -c --comment, Output origin line as python comment')
            sys.exit(0)

        if key in ['-f', '--file']:
            fileName = value

        if key in ['-c', '--comment']:
            origin = True

    try:
        fd = open(fileName, "r")

        printBegin()

        for line in fd.readlines():
            line = line.strip()

            if (origin):
                printf("        #TSIM: %s" % line)

            if (line.find("print") == 0):
                printf("        tdLog.info('%s')" % line.split(' ', 1)[1])

            if (line.find("sql create table") == 0):
                printCreateTable(line)

            if (line.find("sql insert") == 0):
                printInsert(line)

            if (line.find("sql_error") == 0):
                printSqlError(line)

            if (line.find("sql select") == 0):
                printSqlSelect(line)

            if (line.find("sql drop ") == 0):
                printSqlDrop(line)

            if (line.find("sql show") == 0):
                printSqlShow(line)

            if (line.find("sql describe") == 0):
                printSqlDescribe(line)

            if (line.find("if $rows") == 0):
                printIfRows(line)

            if (line.find("if $data") == 0):
                printIfData(line)

        fd.close()
        printf("# convert end")
        printEnd()

    except Exception as e:
        printf("%s %s" % (repr(e), fileName))
