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

#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Sinks/Formats/StatisticCollection/CountMinStatisticFormat.hpp>
#include <Sinks/Formats/StatisticCollection/HyperLogLogStatisticFormat.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::Statistic {
StatisticFormatPtr StatisticFormatFactory::createFromSchema(SchemaPtr schema,
                                                            uint64_t bufferSize,
                                                            StatisticSynopsisType type,
                                                            StatisticDataCodec sinkDataCodec) {
    // 1. We decide what memoryLayout we should use
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    switch (schema->getLayoutType()) {
        case Schema::MemoryLayoutType::ROW_LAYOUT: {
            memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bufferSize);
            break;
        }
        case Schema::MemoryLayoutType::COLUMNAR_LAYOUT: {
            memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bufferSize);
            break;
        }
        default: NES_NOT_IMPLEMENTED();
    }

    // 2. We decide how the post- and preprocessing functions should be, i.e., do we perform some compression for example
    std::function<std::string(const std::string&)> postProcessingData;
    std::function<std::string(const std::string&)> preProcessingData;

    switch (sinkDataCodec) {
        case StatisticDataCodec::DEFAULT: {
            postProcessingData = [](const std::string& data) {
                return data;
            };
            preProcessingData = [](const std::string& data) {
                return data;
            };
            break;
        }
    }

    // 3. We decide what format to build and return the format
    switch (type) {
        case StatisticSynopsisType::COUNT_MIN: return createCountMinFormat(memoryLayout, postProcessingData, preProcessingData);
        case StatisticSynopsisType::HLL: return createHyperLogLogFormat(memoryLayout, postProcessingData, preProcessingData);
    }
}

StatisticFormatPtr
StatisticFormatFactory::createCountMinFormat(const Runtime::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
                                             std::function<std::string(const std::string&)> postProcessingData,
                                             std::function<std::string(const std::string&)> preProcessingData) {
    return CountMinStatisticFormat::create(memoryLayout, postProcessingData, preProcessingData);
}

StatisticFormatPtr
StatisticFormatFactory::createHyperLogLogFormat(const Runtime::MemoryLayouts::MemoryLayoutPtr& memoryLayout,
                                                std::function<std::string(const std::string&)> postProcessingData,
                                                std::function<std::string(const std::string&)> preProcessingData) {
    return HyperLogLogStatisticFormat::create(memoryLayout, postProcessingData, preProcessingData);
}

}// namespace NES::Statistic