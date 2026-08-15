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

#include <Configurations/Validation/ByteAmountValidation.hpp>

#include <regex>
#include <stdexcept>
#include <cmath>

namespace NES
{

Byte parseByteAmount(const std::string& byteAmountStr) {
    if (byteAmountStr.empty()) {
        throw std::invalid_argument("Empty byte amount string");
    }

    std::regex amountRegex(R"(^([0-9]+)(?:\.([0-9]+))?\s*([kKMGTPE]i?)?(b|B)?$)");
    std::smatch match;

    if (!std::regex_match(byteAmountStr, match, amountRegex)) {
        throw std::invalid_argument("Invalid byte amount format");
    }

    std::string intPartStr = match[1];
    std::string fracPartStr = match[2];
    std::string suffix = match[3];

    double multiplier = 1.0;
    if (!suffix.empty()) {
        if (suffix == "k" || suffix == "K") multiplier = 1e3;
        else if (suffix == "M") multiplier = 1e6;
        else if (suffix == "G") multiplier = 1e9;
        else if (suffix == "T") multiplier = 1e12;
        else if (suffix == "P") multiplier = 1e15;
        else if (suffix == "E") multiplier = 1e18;
        else if (suffix == "Ki") multiplier = 1024.0;
        else if (suffix == "Mi") multiplier = std::pow(1024.0, 2);
        else if (suffix == "Gi") multiplier = std::pow(1024.0, 3);
        else if (suffix == "Ti") multiplier = std::pow(1024.0, 4);
        else if (suffix == "Pi") multiplier = std::pow(1024.0, 5);
        else if (suffix == "Ei") multiplier = std::pow(1024.0, 6);
    }

    if (fracPartStr.empty() && suffix.empty()) {
        try {
            uint64_t val = std::stoull(intPartStr);
            return Byte(val);
        } catch(const std::out_of_range&) {
            throw std::overflow_error("Byte amount overflow");
        }
    }

    std::string fullNumStr = intPartStr;
    if (!fracPartStr.empty()) {
        fullNumStr += "." + fracPartStr;
    }
    double number = std::stod(fullNumStr);
    
    double result = number * multiplier;

    if (result < 0) {
        throw std::invalid_argument("Negative byte amount not allowed");
    }
    
    // Check against UINT64_MAX using double conversion
    // Note: UINT64_MAX is 18446744073709551615, which as a double might be rounded up to 18446744073709551616.0
    // So we use result > std::nextafter(18446744073709551615.0, 0.0) or simply >= 18446744073709551616.0
    const double MAX_UINT64_AS_DOUBLE = 18446744073709551616.0; 
    if (result >= MAX_UINT64_AS_DOUBLE) {
        throw std::overflow_error("Byte amount overflow");
    }

    return Byte(static_cast<uint64_t>(std::round(result)));
}

bool ByteAmountValidation::isValid(const std::string& parameter) const
{
    try {
        parseByteAmount(parameter);
        return true;
    } catch (...) {
        return false;
    }
}
}
