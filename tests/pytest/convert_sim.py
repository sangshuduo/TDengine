import re
import sys
import getopt
from distutils.log import warn as printf


tabNum = 0

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


def processFixedString(cmd):
    cmd = cmd.replace("\'", "\"")

    if (("$tb" in cmd) and
        ("$ms" in cmd) and
        ("$x" in cmd)):
        cmd = cmd.replace("$tb", "%s").replace("$ms", "%s").replace("$x", "%d") + "\' % (tb, ms, x))"
    elif (("$tb" in cmd) and
        ("$mt" in cmd)):
        cmd = cmd.replace("$tb", "%s").replace("$mt", "%s") + "\' % (tb, mt))"
    elif ("$tb" in cmd):
        cmd = cmd.replace("$tb", "%s") + "\' % (tb))"
    elif ("$mt" in cmd):
        cmd = cmd.replace("$mt", "%s") + "\' % (mt))"
    else:
        cmd = cmd + "\')"
    return cmd


def printCreateTable(cmd):
    cmd = processFixedString(cmd)

    printf("        %stdLog.info('%s" % (getExtraSpace(), cmd))
    if "-x step" in line:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        %stdSql.error('%s" % (getExtraSpace(), cmd))
    else:
        printf("        %stdSql.execute('%s" % (getExtraSpace(), cmd))


def printInsert(cmd):
    cmd = processFixedString(cmd)

    if "-x step" in cmd:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)

        printf("        %stdLog.info('%s" % (getExtraSpace(), cmd))
        printf("        %stdSql.error('%s" % (getExtraSpace(), cmd))
    else:
        printf("        %stdLog.info('%s" % (getExtraSpace(), cmd))
        printf("        %stdSql.execute('%s" % (getExtraSpace(), cmd))


def printSqlError(cmd):
    cmd = processFixedString(cmd)

    cmd = re.sub(r' -x step[0-9]?', '', cmd)
    printf("        %stdLog.info('%s" % (getExtraSpace(), cmd))
    printf("        %stdSql.error('%s" % (getExtraSpace(), cmd))


def printSqlSelect(cmd):
    cmd = processFixedString(cmd)

    printf("        %stdLog.info('%s" % (getExtraSpace(), cmd))
    if "-x step" in cmd:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        %stdSql.error('%s" % (getExtraSpace(), cmd))
    else:
        printf("        %stdSql.query('%s" % (getExtraSpace(), cmd))


def printSqlAlterTable(cmd):
    cmd = processFixedString(cmd)

    printf("        %stdLog.info('%s" % (getExtraSpace(), cmd))
    if "-x step" in cmd:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        %stdSql.error('%s" % (getExtraSpace(), cmd))
    else:
        printf("        %stdSql.execute('%s" % (getExtraSpace(), cmd))


def printSqlResetQueryCache(cmd):
    printf("        %stdLog.info('%s')" % (getExtraSpace(), cmd))
    if "-x step" in cmd:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        %stdSql.error('%s')" % (getExtraSpace(), cmd))
    else:
        printf("        %stdSql.execute('%s')" % (getExtraSpace(), cmd))


def printSqlDrop(cmd):
    cmd = processFixedString(cmd)

    printf("        %stdLog.info('%s" % (getExtraSpace(), cmd))
    if "-x step" in cmd:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        %stdSql.error('%s" % (getExtraSpace(), cmd))
    else:
        printf("        %stdSql.execute('%s" % (getExtraSpace(), cmd))


def printSqlShow(cmd):
    cmd = processFixedString(cmd)

    printf("        %stdLog.info('%s" % (getExtraSpace(), cmd))
    if "-x step" in cmd:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        %stdSql.error('%s" % (getExtraSpace(), cmd))
    else:
        printf("        %stdSql.query('%s" % (getExtraSpace(), cmd))


def printSqlDescribe(cmd):
    cmd = processFixedString(cmd)

    printf("        %stdLog.info('%s" % (getExtraSpace(), cmd))
    if "-x step" in cmd:
        cmd = re.sub(r' -x step[0-9]?', '', cmd)
        printf("        %stdSql.error('%s" % (getExtraSpace(), cmd))
    else:
        printf("        %stdSql.query('%s" % (getExtraSpace(), cmd))


def printIfRows(cmd):
    expectedRows = cmd.split(' ')[3]
    printf(
        "        %stdLog.info('tdSql.checkRow(%s)')" %
        (getExtraSpace(), expectedRows))
    printf("        %stdSql.checkRows(%s)" % (getExtraSpace(), expectedRows.replace("$", "")))


def getExtraSpace():
    global tabNum

    extraSpace = ""

    i = tabNum
    while (i):
        extraSpace = extraSpace + "    "
        i = i - 1

    return extraSpace


def printWhile(cmd):
    global tabNum

    printf('        %s%s' % (getExtraSpace(), cmd.replace("< $", "< ").replace("$", "(") + "):"))
    tabNum = tabNum + 1


def processEndWhile(cmd):
    global tabNum

    if (tabNum == 0):
        printf("tabNum is 0. Seems tab level processed mistake!")
        sys.exit(1)

    tabNum = tabNum - 1


def printDollarVARms(cmd):
    printf('        %s%s' % (getExtraSpace(), cmd.replace("$", "").replace("x . m", "\"%dm\" % x")))


def printDollarVARtb(cmd):
    printf('        %s%s' % (getExtraSpace(), cmd.replace("$", "").replace(" . ", ", ").replace("tb = ", "tb = \"%s%d\" % (")) + ")")


def printDollarVARmt(cmd):
    printf('        %s%s' % (getExtraSpace(), cmd.replace("$", "").replace(" . ", ", ").replace("mt = ", "mt = \"%s%d\" % (")) + ")")


def printDollarVARtbPrefix(cmd):
    printf('        %s%s' % (getExtraSpace(), cmd.replace("$tbPrefix = ", "tbPrefix = \"") + "\""))


def printDollarVARmtPrefix(cmd):
    printf('        %s%s' % (getExtraSpace(), cmd.replace("$mtPrefix = ", "mtPrefix = \"") + "\""))


def printDollarVAR(cmd):
    printf('        %s%s' % (getExtraSpace(), cmd.replace("$", "")))


def printIfData(cmd):
    colAndRow = cmd.split(' ')[1].replace("$data", "")
    checkCol = colAndRow[0:1]
    checkRow = colAndRow[1:2]

    if "@" in cmd:
        expectedData = re.search('@(.*)@', cmd).group(1)
        printf(
            '        tdLog.info("tdSql.checkData(%s, %s, %s)")' %
            (checkCol, checkRow, expectedData))

        printf('        expectedData = datetime.datetime.strptime("%s", '
               '"%%y-%%m-%%d %%H:%%M:%%S.%%f")' % expectedData)
        printf("        tdSql.checkData(%s, %s, %s)" %
               (checkCol, checkRow, "expectedData"))
    else:
        expectedData = cmd.split(' ')[3]
        printf(
            "        tdLog.info('tdSql.checkData(%s, %s, %s)')" %
            (checkCol, checkRow, expectedData))

        if (expectedData.lower() == "null"):
            printf("        tdSql.checkData(%s, %s, %s)" %
                   (checkCol, checkRow, "None"))
        else:
            printf("        tdSql.checkData(%s, %s, %s)" %
                   (checkCol, checkRow, expectedData))


if __name__ == "__main__":
    origin = False
    i = 0

    opts, args = getopt.gnu_getopt(sys.argv[1:], 'f:ch', [
        'file=', 'comment', 'help'])

    if (opts == []):
        opts = (['--help', ''], [])

    for key, value in opts:
        if (key in ['-h', '--help']):
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
                printf("        %s#TSIM: %s" % (getExtraSpace(), line))

            if (line.find("print") == 0):
                printf("        tdLog.info('%s')" % line.split(' ', 1)[1])

            if (line.find("sql create table") == 0):
                line = line.split(' ', 1)[1]
                printCreateTable(line)

            if (line.find("sql_error") == 0):
                line = line.split(' ', 1)[1]
                printSqlError(line)

            if ((line.find("sql ") == 0) and (line.find("insert") != -1)):
                line = line.split(' ', 1)[1]
                printInsert(line)

            if (line.find("sql select") == 0):
                line = line.split(' ', 1)[1]
                printSqlSelect(line)

            if (line.find("sql drop ") == 0):
                printSqlDrop(line)

            if (line.find("sql reset query cache") == 0):
                line = line.split(' ', 1)[1]
                printSqlResetQueryCache(line)

            if (line.find("sql alter table") == 0):
                line = line.split(' ', 1)[1]
                printSqlAlterTable(line)

            if (line.find("sql show") == 0):
                line = line.split(' ', 1)[1]
                printSqlShow(line)

            if (line.find("sql describe") == 0):
                line = line.split(' ', 1)[1]
                printSqlDescribe(line)

            if (line.find("if $rows") == 0):
                printIfRows(line)

            if (line.find("if $data") == 0):
                printIfData(line)

            if (line.find("while $") == 0):
                printWhile(line)

            if (line.find("endw") == 0):
                processEndWhile(line)

            if ("$mt = " in line):
                printDollarVARmt(line)

            if ("$tb = " in line):
                printDollarVARtb(line)

            if (line.find("$tbPrefix = ") ==0):
                printDollarVARtbPrefix(line)

            if ("$ms = " in line):
                printDollarVARms(line)

            if (line.find("$mtPrefix = ") ==0):
                printDollarVARmtPrefix(line)

            if ((line.find("$i = ") ==0) or
                (line.find("$x = ") ==0) or
                (line.find("$tbNum = ") ==0) or
                (line.find("$rowNum = ") ==0) or
                    (line.find("$totalNum = ") ==0)):
                printDollarVAR(line)

        fd.close()
        printf("# convert end")
        printEnd()

    except Exception as e:
        printf("%s %s" % (repr(e), fileName))
