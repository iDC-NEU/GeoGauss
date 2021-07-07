/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * utilities.cpp
 *    MOT utilities.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/utils/utilities.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <arpa/inet.h>
#include <errno.h>
#include "sys_numa_api.h"
#include <string.h>
#include <time.h>
#include <math.h>
#include <sys/syscall.h>
#include <execinfo.h>
#include <algorithm>
#include <cstdarg>

#include "table.h"
#include "utilities.h"
#include "table.h"

#include "mot_list.h"
#include "mot_map.h"
#include "mot_string.h"

#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <array>

namespace MOT {
constexpr char hexMap[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
//ADDBY NEU
std::string ExecOsCommand(const std::string& cmd)
{
    return ExecOsCommand(cmd.c_str());
}

std::string ExecOsCommand(const char* cmd)
{
    char buffer[128];
    std::string result;
    std::shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
    if (!pipe)
        return nullptr;
    while (!feof(pipe.get())) {
        if (fgets(buffer, 128, pipe.get()) != nullptr)
            result += buffer;
    }
    return result;
}

std::string HexStr(const uint8_t* data, uint16_t len)
{
    std::string outStr(len * 2, ' ');
    uint32_t pos = 0;
    for (int i = 0; i < len; ++i) {
        outStr[pos++] = hexMap[HIGH_NIBBLE(data[i])];
        outStr[pos++] = hexMap[LOW_NIBBLE(data[i])];
    }
    return outStr;
}
//ADDBY NEU
uint8_t* StrToUint(std::string data){
    uint8_t m_keyBuf[data.size()/2];
    uint32_t pos = 0;
    for (size_t i = 0; i < data.size(); i++){
            m_keyBuf[pos++] = (((uint8_t)(data[i] - '0') & 0x0F) << 4) | (((uint8_t)(data[++i] - '0') & 0x0F));
    }
    return (uint8_t*)m_keyBuf;
}
uint64_t now_to_us()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
}  // namespace MOT
