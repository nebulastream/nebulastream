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

#ifndef NES_INCLUDE_CATALOGS_UDF_PYTHONUDFDESCRIPTOR_HPP_
#define NES_INCLUDE_CATALOGS_UDF_PYTHONUDFDESCRIPTOR_HPP_

#include <string>
#include <memory>
#include <Common/DataTypes/DataType.hpp>
#include <Catalogs/UDF/UdfDescriptor.hpp>

namespace NES::Catalogs {

class PythonUdfDescriptor;
using PythonUdfDescriptorPtr = std::shared_ptr<PythonUdfDescriptor>;

class PythonUdfDescriptor : public UdfDescriptor {
public:

PythonUdfDescriptor(const std::string& methodName,
                    int numberOfArgs,
                    DataTypePtr& returnType);

static PythonUdfDescriptorPtr create(const std::string& methodName,
                                     int numberOfArgs,
                                     DataTypePtr& returnType) {
    return std::make_shared<PythonUdfDescriptor>(methodName, numberOfArgs, returnType);
}

/**
 * @brief Return the number of arguments for the UDF.
 * @return The number of arguments of the UDF method.
 */
[[nodiscard]] int getNumberOfArgs() const { return numberOfArgs; }

/**
 * @brief Return the number of arguments for the UDF.
 * @return The name of the UDF method.
 */
[[nodiscard]] DataTypePtr getReturnType() const { return returnType; }

private:
const int numberOfArgs;
const DataTypePtr returnType;
};
}// namespace NES::Catalogs
#endif// NES_INCLUDE_CATALOGS_UDF_PYTHONUDFDESCRIPTOR_HPP_