/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_BENCHMARK_INCLUDE_UTIL_BENCHMARK_SCHEMAS_HPP_
#define NES_BENCHMARK_INCLUDE_UTIL_BENCHMARK_SCHEMAS_HPP_

#include <API/Schema.hpp>
#include <vector>

namespace NES::Benchmarking {
class BenchmarkSchemas {
  public:
    static std::vector<SchemaPtr> getBenchmarkSchemas() {
        return std::vector<SchemaPtr>({
            Schema::create()->addField("test$key", BasicType::INT16)->addField("test$value", BasicType::INT8), //  3 Byte
            Schema::create()->addField("test$key", BasicType::INT16)->addField("test$value", BasicType::INT16),//  4 Byte
            Schema::create()->addField("test$key", BasicType::INT32)->addField("test$value", BasicType::INT8), //  5 Byte
            Schema::create()->addField("test$key", BasicType::INT32)->addField("test$value", BasicType::INT16),//  6 Byte
            Schema::create()
                ->addField("test$key", BasicType::INT32)
                ->addField("test$value", BasicType::INT8)//  7 Byte
                ->addField("test$value1", BasicType::INT8),

            Schema::create()->addField("test$key", BasicType::INT32)->addField("test$value", BasicType::INT32),//  8 Byte
            Schema::create()->addField("test$key", BasicType::INT64)->addField("test$value", BasicType::INT8), //  9 Byte
            Schema::create()->addField("test$key", BasicType::INT64)->addField("test$value", BasicType::INT32),// 10 Byte
            Schema::create()->addField("test$key", BasicType::INT64)->addField("test$value", BasicType::INT64),// 16 Byte
            Schema::create()
                ->addField("test$key", BasicType::INT64)
                ->addField("test$value", BasicType::INT64)// 32 Byte
                ->addField("test$value1", BasicType::INT64)
                ->addField("test$value2", BasicType::INT64),

            Schema::create()
                ->addField("test$key", BasicType::INT64)
                ->addField("test$value", BasicType::INT64)// 64 Byte
                ->addField("test$value1", BasicType::INT64)
                ->addField("test$value2", BasicType::INT64)
                ->addField("test$value3", BasicType::INT64)
                ->addField("test$value4", BasicType::INT64)
                ->addField("test$value5", BasicType::INT64)
                ->addField("test$value6", BasicType::INT64),

            Schema::create()
                ->addField("test$key", BasicType::INT64)
                ->addField("test$value", BasicType::INT64)//128 Byte
                ->addField("test$value1", BasicType::INT64)
                ->addField("test$value2", BasicType::INT64)
                ->addField("test$value3", BasicType::INT64)
                ->addField("test$value4", BasicType::INT64)
                ->addField("test$value5", BasicType::INT64)
                ->addField("test$value6", BasicType::INT64)
                ->addField("test$value7", BasicType::INT64)
                ->addField("test$value8", BasicType::INT64)
                ->addField("test$value9", BasicType::INT64)
                ->addField("test$value10", BasicType::INT64)
                ->addField("test$value11", BasicType::INT64)
                ->addField("test$value12", BasicType::INT64)
                ->addField("test$value13", BasicType::INT64)
                ->addField("test$value14", BasicType::INT64),
        });
    }
};
}// namespace NES::Benchmarking
#endif// NES_BENCHMARK_INCLUDE_UTIL_BENCHMARK_SCHEMAS_HPP_
