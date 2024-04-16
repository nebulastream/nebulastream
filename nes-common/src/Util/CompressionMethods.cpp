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

#include <Util/CompressionMethods.hpp>
#include <Util/StdInt.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {
/**
 * @brief Small struct that helps for run length de- and encoding
 */
struct __attribute__ ((packed)) RunLength {
    uint64_t count;
    char byte;
};

std::string CompressionMethods::runLengthEncoding(const std::string& data) {
    // This is not the best or most efficient optimization, but it is a simple one. If we want utmost efficiency, we should
    // have this as a nautilus operator

    auto count = 0_u64;
    char charToCompare = data[0];
    std::vector<RunLength> runLengths;
    for (const auto& byte : data) {
        if (byte == charToCompare) {
            ++count;
        } else {
            runLengths.push_back({count, charToCompare});
            count = 1;
            charToCompare = byte;
        }
    }
    runLengths.push_back({count, charToCompare});

    // We now have to copy the data into a string
    std::string encodedStr;
    const auto dataSizeBytes = runLengths.size() * sizeof(RunLength);
    encodedStr.resize(dataSizeBytes);
    std::memcpy(encodedStr.data(), runLengths.data(), dataSizeBytes);

    return encodedStr;
}

std::string CompressionMethods::runLengthDecoding(const std::string& data) {
    // This is not the best or most efficient optimization, but it is a simple one. If we want utmost efficiency, we should
    // have this as a nautilus operator
    std::string decodedStr;
    const auto* runLengths = reinterpret_cast<const RunLength*>(data.data());
    for (auto cntRunLength = 0_u64; cntRunLength < data.size() / sizeof(RunLength); ++cntRunLength) {
        for (auto i = 0_u64; i < runLengths[cntRunLength].count; i++) {
            decodedStr += runLengths[cntRunLength].byte;
        }
    }

    return decodedStr;
}

}// namespace NES