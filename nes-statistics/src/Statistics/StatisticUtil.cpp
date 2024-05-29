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
#include <API/Schema.hpp>
#include <StatisticIdentifiers.hpp>
#include <Statistics/StatisticUtil.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Util/Common.hpp>
#include <Util/StdInt.hpp>
#include <random>
#include <string>

namespace NES::Statistic {

uint64_t StatisticUtil::getH3HashValue(BasicValue& value, uint64_t row, uint64_t depth, uint64_t numberOfBitsInKey) {
    // Creating here the H3-Seeds with the same seed, as used in the creation of the count min sketch
    std::random_device rd;
    std::mt19937 gen(H3_SEED);
    std::uniform_int_distribution<uint64_t> distribution;
    std::vector<uint64_t> h3Seeds;
    for (auto tmpRow = 0UL; tmpRow < depth; ++tmpRow) {
        for (auto keyBit = 0UL; keyBit < numberOfBitsInKey; ++keyBit) {
            h3Seeds.emplace_back(distribution(gen));
        }
    }

    // We do not require the data type but only the size, as H3 operators on the bits and not on the value
    uint64_t key = 0;
    if (value.dataType->isInteger()) {
        auto tmpKey = std::stoull(value.value);
        std::memcpy(&key, &tmpKey, sizeof(uint64_t));
    } else if (value.dataType->isFloat()) {
        auto tmpKey = std::stod(value.value);
        std::memcpy(&key, &tmpKey, sizeof(uint64_t));
    } else {
        NES_NOT_IMPLEMENTED();
    }

    // Calculate the hash value
    const auto h3SeedsOffSet = (row * numberOfBitsInKey);
    uint64_t hash = 0;
    for (auto i = 0_u64; i < numberOfBitsInKey; i = i + 1) {
        const auto isBitSet = (key >> i) & 1;
        const auto h3Seed = h3Seeds[h3SeedsOffSet + i];
        hash = hash ^ (isBitSet * h3Seed);
    }

    return hash;
}

SchemaPtr StatisticUtil::createSampleSchema(const Schema& inputSchema) {
    // We assume that the sampleMemoryLayout is always a column layout and has the same order as the memory layout
    const auto qualifierNameWithSeparator = inputSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator();
    auto sampleSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    sampleSchema->copyFields(inputSchema); // for doing a deep copy

    // Removing all fields that are part of the statistic schema. Maybe, someone has a better idea to do this
    sampleSchema->removeField(qualifierNameWithSeparator + BASE_FIELD_NAME_START);
    sampleSchema->removeField(qualifierNameWithSeparator + BASE_FIELD_NAME_END);
    sampleSchema->removeField(qualifierNameWithSeparator + STATISTIC_HASH_FIELD_NAME);
    sampleSchema->removeField(qualifierNameWithSeparator + OBSERVED_TUPLES_FIELD_NAME);
    sampleSchema->removeField(qualifierNameWithSeparator + STATISTIC_TYPE_FIELD_NAME);
    sampleSchema->removeField(qualifierNameWithSeparator + WIDTH_FIELD_NAME);
    sampleSchema->removeField(qualifierNameWithSeparator + STATISTIC_DATA_FIELD_NAME);
    return sampleSchema;
}

bool StatisticUtil::compareFieldWithBasicValue(const int8_t* leftField, const BasicValue& value) {
    // For now, we only support integers and floats
    if (value.dataType->isInteger()) {
        auto tmpValue = std::stoull(value.value);
        return *reinterpret_cast<const uint64_t*>(leftField) == tmpValue;
    } else if (value.dataType->isFloat()) {
        auto tmpValue = std::stod(value.value);
        return *reinterpret_cast<const double*>(leftField) == tmpValue;
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

}// namespace NES::Statistic
