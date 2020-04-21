/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_MGMT_LOG_H
#define TDENGINE_MGMT_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

extern int32_t mdebugFlag;
extern int32_t sdbDebugFlag;

// mnode log function
#define mError(...)                          \
  if (mdebugFlag & DEBUG_ERROR) {            \
    taosPrintLog("ERROR MND ", 255, __VA_ARGS__); \
  }
#define mWarn(...)                                  \
  if (mdebugFlag & DEBUG_WARN) {                    \
    taosPrintLog("WARN  MND ", mdebugFlag, __VA_ARGS__); \
  }
#define mTrace(...)                           \
  if (mdebugFlag & DEBUG_TRACE) {             \
    taosPrintLog("MND ", mdebugFlag, __VA_ARGS__); \
  }
#define mPrint(...) \
  { taosPrintLog("MND ", 255, __VA_ARGS__); }

#define mLError(...) mError(__VA_ARGS__)
#define mLWarn(...)  mWarn(__VA_ARGS__)
#define mLPrint(...) mPrint(__VA_ARGS__)

#define sdbError(...)                            \
  if (sdbDebugFlag & DEBUG_ERROR) {              \
    taosPrintLog("ERROR MND-SDB ", 255, __VA_ARGS__); \
  }
#define sdbWarn(...)                                      \
  if (sdbDebugFlag & DEBUG_WARN) {                        \
    taosPrintLog("WARN  MND-SDB ", sdbDebugFlag, __VA_ARGS__); \
  }
#define sdbTrace(...)                               \
  if (sdbDebugFlag & DEBUG_TRACE) {                 \
    taosPrintLog("MND-SDB ", sdbDebugFlag, __VA_ARGS__); \
  }
#define sdbPrint(...) \
  { taosPrintLog("MND-SDB ", 255, __VA_ARGS__); }

#define sdbLError(...) sdbError(__VA_ARGS__)
#define sdbLWarn(...)  sdbWarn(__VA_ARGS__)
#define sdbLPrint(...) sdbPrint(__VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif
