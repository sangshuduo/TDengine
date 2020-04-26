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


if __name__ == "__main__":
    origin = False

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
                cmd = line.split(' ', 1)[1]
                printf("        tdLog.info('%s')" % cmd)
                printf("        tdSql.execute('%s')" % cmd)

            if (line.find("sql insert") == 0):
                cmd = line.split(' ', 1)[1]
                printf('        tdLog.info("%s")' % cmd)
                printf('        tdSql.execute("%s")' % cmd)

            if (line.find("sql select") == 0):
                cmd = line.split(' ', 1)[1]
                printf("        tdLog.info('%s')" % cmd)
                printf("        tdSql.query('%s')" % cmd)

            if (line.find("if $rows") == 0):
                expectedRows = line.split(' ')[3]
                printf(
                    "        tdLog.info('tdSql.checkRow(%s)')" %
                    expectedRows)
                printf("        tdSql.checkRows(%s)" % expectedRows)

        fd.close()
        printf("# convert end")
        printEnd()

    except Exception as e:
        printf("%s %s" % (repr(e), fileName))
