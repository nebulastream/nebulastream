//
// Created by nils on 04.12.20.
//

#ifndef NES_BENCHMARKSCHEMAS_HPP
#define NES_BENCHMARKSCHEMAS_HPP

#include <vector>
#include <API/Schema.hpp>


namespace NES::Benchmarking {
class BenchmarkSchemas {
  public:
    static std::vector<SchemaPtr> getBenchmarkSchemas() {
        return std::vector<SchemaPtr>({
            Schema::create()->addField("key", BasicType::INT16)->addField("value", BasicType::INT8),        //  3 Byte
            Schema::create()->addField("key", BasicType::INT16)->addField("value", BasicType::INT16),       //  4 Byte
            /*Schema::create()->addField("key", BasicType::INT32)->addField("value", BasicType::INT8),        //  5 Byte
            Schema::create()->addField("key", BasicType::INT32)->addField("value", BasicType::INT16),       //  6 Byte
            Schema::create()->addField("key", BasicType::INT32)->addField("value", BasicType::INT8)                    //  7 Byte
                            ->addField("value1", BasicType::INT8),

            Schema::create()->addField("key", BasicType::INT32)->addField("value", BasicType::INT32),       //  8 Byte
            Schema::create()->addField("key", BasicType::INT64)->addField("value", BasicType::INT8),        //  9 Byte
            Schema::create()->addField("key", BasicType::INT64)->addField("value", BasicType::INT32),       // 10 Byte
            Schema::create()->addField("key", BasicType::INT64)->addField("value", BasicType::INT64),       // 16 Byte
            Schema::create()->addField("key", BasicType::INT64)->addField("value", BasicType::INT64)                   // 32 Byte
                            ->addField("value1", BasicType::INT64)->addField("value2", BasicType::INT64),

            Schema::create()->addField("key", BasicType::INT64)->addField("value", BasicType::INT64)                   // 64 Byte
                            ->addField("value1", BasicType::INT64)->addField("value2", BasicType::INT64)
                            ->addField("value3", BasicType::INT64)->addField("value4", BasicType::INT64)
                            ->addField("value5", BasicType::INT64)->addField("value6", BasicType::INT64),

            Schema::create()->addField("key", BasicType::INT64)->addField("value", BasicType::INT64)                    //128 Byte
                            ->addField("value1", BasicType::INT64)->addField("value2", BasicType::INT64)
                            ->addField("value3", BasicType::INT64)->addField("value4", BasicType::INT64)
                            ->addField("value5", BasicType::INT64)->addField("value6", BasicType::INT64)
                            ->addField("value7", BasicType::INT64)->addField("value8", BasicType::INT64)
                            ->addField("value9", BasicType::INT64)->addField("value10", BasicType::INT64)
                            ->addField("value11", BasicType::INT64)->addField("value12", BasicType::INT64)
                            ->addField("value13", BasicType::INT64)->addField("value14", BasicType::INT64),*/
        });
    }
};
}
#endif//NES_BENCHMARKSCHEMAS_HPP
