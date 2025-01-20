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
#include <API/TestSchemas.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
SchemaPtr TestSchemas::getSchemaTemplate(const std::string& name)
{
    auto it = testSchemaCatalog.find(name);
    PRECONDITION(it != testSchemaCatalog.end(), "Schema with name: {} not found", name);
    auto newSchema = std::make_shared<Schema>();
    /// Use copyFields() to create a deep copy of the fields
    newSchema->copyFields(it->second);
    return newSchema;
}

std::unordered_map<std::string, SchemaPtr> NES::TestSchemas::testSchemaCatalog = {
    {"id_u64", Schema::create()->addField("id", BasicType::UINT64)},
    {"id_time_u64", Schema::create()->addField("id", BasicType::UINT64)->addField("timestamp", BasicType::UINT64)},
    {"id2_time_u64", Schema::create()->addField("id2", BasicType::UINT64)->addField("timestamp", BasicType::UINT64)},
    {"id_val_64", Schema::create()->addField("id", BasicType::INT64)->addField("value", BasicType::INT64)},
    {"id_val_u64", Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)},
    {"id_val_u32", Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT32)},
    {"id_val_time_u64",
     Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("timestamp", BasicType::UINT64)},
    {"id2_val2_time_u64",
     Schema::create()->addField("id2", BasicType::UINT64)->addField("value2", BasicType::UINT64)->addField("timestamp", BasicType::UINT64)},
    {"id3_val3_time_u64",
     Schema::create()->addField("id3", BasicType::UINT64)->addField("value3", BasicType::UINT64)->addField("timestamp", BasicType::UINT64)},
    {"id4_val4_time_u64",
     Schema::create()->addField("id4", BasicType::UINT64)->addField("value4", BasicType::UINT64)->addField("timestamp", BasicType::UINT64)},
    {"id_val_time_u32",
     Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT32)->addField("timestamp", BasicType::UINT64)},
    {"id_one_val_64",
     Schema::create()->addField("id", BasicType::INT64)->addField("one", BasicType::INT64)->addField("value", BasicType::INT64)},
    {"id_2val_time_u64",
     Schema::create()
         ->addField("id", BasicType::UINT64)
         ->addField("value", BasicType::UINT64)
         ->addField("value2", BasicType::UINT64)
         ->addField("timestamp", BasicType::UINT64)}};
}
