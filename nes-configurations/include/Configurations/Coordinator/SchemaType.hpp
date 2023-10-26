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

#ifndef NES_SCHEMATYPE_HPP
#define NES_SCHEMATYPE_HPP

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace NES::Configurations {

struct SchemaFieldDetail {
  public:
    SchemaFieldDetail(std::string fieldName, std::string fieldType, std::string variableLengthInBytes = "0")
        : fieldName(std::move(fieldName)), fieldType(std::move(fieldType)),
          variableLengthInBytes(std::move(variableLengthInBytes)){};
    std::string fieldName;
    std::string fieldType;
    std::string variableLengthInBytes;
};

class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;

/**
 * @brief This class is a wrapper for storing the schema related configurations supplied by user and then later using it to create the actual schema
 */
class SchemaType {
  public:
    static SchemaTypePtr create(const std::vector<SchemaFieldDetail>& schemaFieldDetails);

    [[nodiscard]] const std::vector<SchemaFieldDetail>& getSchemaFieldDetails() const;

    bool contains(const std::string& fieldName);

  private:
    explicit SchemaType(std::vector<SchemaFieldDetail> schemaFieldDetails);
    std::vector<SchemaFieldDetail> schemaFieldDetails;
};
}// namespace NES::Configurations

#endif//NES_SCHEMATYPE_HPP
