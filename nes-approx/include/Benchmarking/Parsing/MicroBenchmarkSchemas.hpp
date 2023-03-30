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

#include <API/Schema.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>

namespace NES::ASP::Benchmarking {

static inline std::map<std::string, SchemaPtr> inputFileSchemas = {
    {"some_input_file", Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                    ->addField("id", BasicType::UINT64)
                    ->addField("value", BasicType::INT64)
                    ->addField("timestamp", BasicType::UINT64)},

    {"some_other_input_file", Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                    ->addField("id", BasicType::UINT64)
                    ->addField("value1", BasicType::INT64)
                    ->addField("value2", BasicType::FLOAT32)
                    ->addField("value3", BasicType::BOOLEAN)
                    ->addField("timestamp", BasicType::UINT64)}};

static inline std::map<std::string, SchemaPtr> accuracyFileSchemas = {
    {"some_min_acc_file", Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                            ->addField("aggregation", BasicType::INT64)},

    {"some_max_acc_file", Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                  ->addField("aggregation", BasicType::INT64)},

    {"some_sum_acc_file", Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                              ->addField("aggregation", BasicType::INT64)},

    {"some_average_acc_file", Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                              ->addField("aggregation", BasicType::INT64)},

    {"some_count_acc_file", Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                              ->addField("aggregation", BasicType::INT64)}};

} // namespace NES::ASP::Benchmarking

#endif//NES_MICROBENCHMARKSCHEMAS_HPP
