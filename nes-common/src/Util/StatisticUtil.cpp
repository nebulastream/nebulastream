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

#include <Util/Logger/Logger.hpp>
#include <Util/StatisticUtil.hpp>
#include <fstream>
#include <sstream>
#include <vector>

namespace NES::Experimental::Statistics {

std::vector<uint64_t> StatisticUtil::readFlattenedVectorFromCsvFile(const std::string& filename) {
    std::vector<uint64_t> csvData;

    std::ifstream file(filename);

    if (!file.is_open()) {
        NES_ERROR("Could not open file: {}\n", filename);
        return csvData;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string token;
        while (std::getline(iss, token, ',')) {
            uint64_t value = std::stoull(token);
            csvData.push_back(value);
        }
    }
    file.close();

    return csvData;
}

std::vector<std::vector<uint64_t>> StatisticUtil::read2DVectorFromCsvFile(const std::string& filename) {
    std::vector<std::vector<uint64_t>> csvData;

    std::ifstream file(filename);
    if (!file.is_open()) {
        NES_ERROR("Could not open file: {}\n", filename);
        return csvData;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::vector<uint64_t> rowData;
        std::istringstream iss(line);
        std::string token;

        while (std::getline(iss, token, ',')) {
            uint64_t value = std::stoull(token);
            rowData.push_back(value);
        }
        csvData.push_back(rowData);
    }
    file.close();

    return csvData;
}
}// namespace NES::Experimental::Statistics