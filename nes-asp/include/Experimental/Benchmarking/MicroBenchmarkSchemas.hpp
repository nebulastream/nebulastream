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

#ifndef NES_MICROBENCHMARKSCHEMAS_HPP
#define NES_MICROBENCHMARKSCHEMAS_HPP

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Experimental/Parsing/SynopsisAggregationConfig.hpp>

namespace NES::ASP::Benchmarking {

/**
 * @brief This map is used for mapping from input files to schema. We do not put the whole path, but stick with the file name.
 */
static inline std::map<std::string, SchemaPtr> inputFileSchemas = {
    {"uniform_key_value_timestamp.csv", Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                    ->addField("id", BasicType::UINT64)
                    ->addField("value", BasicType::INT64)
                    ->addField("ts", BasicType::UINT64)},

    {"uniform_key_multiple_values_timestamp.csv", Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                    ->addField("id", BasicType::UINT64)
                    ->addField("value1", BasicType::INT64)
                    ->addField("value2", BasicType::FLOAT32)
                    ->addField("value3", BasicType::BOOLEAN)
                    ->addField("ts", BasicType::UINT64)}};

/**
 * @brief Retrieves the output schema from the aggregation type and input schema. The data type of the output field is the same
 * as of the input schema, except for AGGREGATION_TYPE::AVERAGE, where it is a BasicType::FLOAT64
 * @param type
 * @param inputSchema
 * @param fieldNameAgg
 * @return Output schema
 */
static inline SchemaPtr getOutputSchemaFromTypeAndInputSchema(Parsing::AGGREGATION_TYPE type, Schema& inputSchema,
                                                              const std::string& fieldNameAgg) {
    switch(type) {
        case Parsing::AGGREGATION_TYPE::NONE:
        case Parsing::AGGREGATION_TYPE::MIN:
        case Parsing::AGGREGATION_TYPE::MAX:
        case Parsing::AGGREGATION_TYPE::SUM:
        case Parsing::AGGREGATION_TYPE::COUNT:
            return Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                ->addField("aggregation", inputSchema.get(fieldNameAgg)->getDataType());
        case Parsing::AGGREGATION_TYPE::AVERAGE:
            return Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                ->addField("aggregation", BasicType::FLOAT64);
    }
}

} // namespace NES::ASP::Benchmarking

#endif//NES_MICROBENCHMARKSCHEMAS_HPP
