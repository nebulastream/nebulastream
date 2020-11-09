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

#ifndef NES_TESTS_UTIL_SCHEMASOURCEDESCRIPTOR_HPP_
#define NES_TESTS_UTIL_SCHEMASOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <API/Schema.hpp>
namespace NES{

class SchemaSourceDescriptor: public SourceDescriptor{
  public:
    static SourceDescriptorPtr create(SchemaPtr schema){
        return std::make_shared<SchemaSourceDescriptor>(schema);
    }
    explicit SchemaSourceDescriptor(SchemaPtr schema): SourceDescriptor(schema, 1){

    }
    std::string toString() override {
        return "Schema Source Descriptor";
    }
    bool equal(SourceDescriptorPtr other) override {
        return other->getSchema()->equals(this->getSchema());
    }
    ~SchemaSourceDescriptor() override = default;
};


}

#endif//NES_TESTS_UTIL_SCHEMASOURCEDESCRIPTOR_HPP_
