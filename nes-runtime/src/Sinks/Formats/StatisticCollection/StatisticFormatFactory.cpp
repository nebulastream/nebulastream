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

#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Sinks/Formats/StatisticCollection/CountMinStatisticFormat.hpp>
#include <Sinks/Formats/StatisticCollection/HyperLogLogStatisticFormat.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {
AbstractStatisticFormatPtr
StatisticFormatFactory::createFromSchema(SchemaPtr schema, uint64_t bufferSize, StatisticSinkFormatType type) {
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

    // 2. We decide what format to build and return the format
    switch (type) {
        case StatisticSinkFormatType::COUNT_MIN: return createCountMinFormat(memoryLayout);
        case StatisticSinkFormatType::HLL: return createHyperLogLogFormat(memoryLayout);
    }
}

AbstractStatisticFormatPtr
StatisticFormatFactory::createCountMinFormat(const Runtime::MemoryLayouts::MemoryLayoutPtr& memoryLayout) {
    return CountMinStatisticFormat::create(memoryLayout);
}

AbstractStatisticFormatPtr
StatisticFormatFactory::createHyperLogLogFormat(const Runtime::MemoryLayouts::MemoryLayoutPtr& memoryLayout) {
    return HyperLogLogStatisticFormat::create(memoryLayout);
}

}// namespace NES::Statistic