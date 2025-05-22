/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <ios>
#include <numeric>
#include <ostream>
#include <sstream>
#include <string>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES
{
VerbosityLevel getVerbosityLevel(std::ostream& os)
{
    const auto value = os.iword(getIwordIndex());
    const auto optional = magic_enum::enum_cast<VerbosityLevel>(value);
    if (optional.has_value())
    {
        return optional.value();
    }
    NES_ERROR("Could not cast `{}` to VerbosityLevel. Will return {}", value, magic_enum::enum_name(VerbosityLevel::Debug));
    return VerbosityLevel::Debug;
}

void setVerbosityLevel(std::ostream& os, const VerbosityLevel& level)
{
    os.iword(getIwordIndex()) = magic_enum::enum_integer(level);
}

int getIwordIndex()
{
    static const int iwordIndex = std::ios_base::xalloc();
    return iwordIndex;
}
}

namespace NES::Util
{

std::string escapeJson(const std::string& str)
{
    std::ostringstream os;
    for (char character : str)
    {
        if (character == '"' || character == '\\' || ('\x00' <= character && character <= '\x1f'))
        {
            os << "\\u" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(character);
        }
        else
        {
            os << character;
        }
    }
    return os.str();
}

void writeHeaderToCsvFile(const std::string& csvFileName, const std::string& header)
{
    std::ofstream ofstream(csvFileName, std::ios::trunc | std::ios::out);
    ofstream << header << std::endl;
    ofstream.close();
}

void writeRowToCsvFile(const std::string& csvFileName, const std::string& row)
{
    std::ofstream ofstream(csvFileName, std::ios::app | std::ios::out);
    ofstream << row << std::endl;
    ofstream.close();
}

std::string updateSourceName(std::string queryPlanSourceConsumed, std::string subQueryPlanSourceConsumed)
{
    ///Update the Source names by sorting and then concatenating the source names from the sub query plan
    std::vector<std::string> sourceNames;
    sourceNames.emplace_back(subQueryPlanSourceConsumed);
    sourceNames.emplace_back(queryPlanSourceConsumed);
    std::ranges::sort(sourceNames);
    /// accumulating sourceNames with delimiters between all sourceNames to enable backtracking of origin
    auto updatedSourceName = std::accumulate(
        sourceNames.begin(), sourceNames.end(), std::string("-"), [](std::string a, std::string b) { return a + "_" + b; });
    return updatedSourceName;
}

uint64_t murmurHash(const uint64_t key)
{
    uint64_t hash = key;

    hash ^= hash >> 33;
    hash *= UINT64_C(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash *= UINT64_C(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;

    return hash;
}

uint64_t countLines(const std::string& str)
{
    std::stringstream stream(str);
    return countLines(stream);
}

uint64_t countLines(std::istream& stream)
{
    std::string tmpStr;
    uint64_t cnt = 0;
    while (std::getline(stream, tmpStr))
    {
        ++cnt;
    }

    return cnt;
}

}
