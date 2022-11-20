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

#ifndef NES_CORE_INCLUDE_CATALOGS_UDF_UDFDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_CATALOGS_UDF_UDFDESCRIPTOR_HPP_

#include <Common/DataTypes/DataType.hpp>
#include <memory>
#include <string>
#include <utility>

namespace NES::Catalogs::UDF {

class UdfDescriptor;
using UdfDescriptorPtr = std::shared_ptr<UdfDescriptor>;

class UdfDescriptor {
  public:
    explicit UdfDescriptor(const std::string& methodName) : methodName(methodName){};

    virtual ~UdfDescriptor() = default;

    /**
    * @brief Return the name of the UDF method.
    * @return The name of the UDF method.
    */
    [[nodiscard]] const std::string& getMethodName() const { return methodName; }

    /**
    * @brief Retrieve the return type of the UDF
    * @return A DataType pointer for the UDF return type
    */
    [[nodiscard]] DataTypePtr getReturnType() const { return returnType; }

    template<class UdfDescriptor>
    static std::shared_ptr<UdfDescriptor> as(UdfDescriptorPtr ptr) {
        return std::dynamic_pointer_cast<UdfDescriptor>(ptr);
    }

  private:
    const std::string methodName;
    const DataTypePtr returnType;
};
}// namespace NES::Catalogs::UDF
#endif// NES_CORE_INCLUDE_CATALOGS_UDF_UDFDESCRIPTOR_HPP_
