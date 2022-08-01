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

#include <Catalogs/UDF/PythonUdfDescriptor.hpp>
#include <Exceptions/UdfException.hpp>

namespace NES::Catalogs::UDF {

PythonUdfDescriptor::PythonUdfDescriptor(const std::string& methodName, int numberOfArgs, DataTypePtr& returnType)
    : UdfDescriptor(methodName), numberOfArgs(numberOfArgs), returnType(returnType) {
    if (methodName.empty()) {
        throw UdfException("The method name of a Python UDF must not be empty");
    }
    // Note: For python >= 3.7 there is no limit on the number of arguments
    if (numberOfArgs < 0 || numberOfArgs > 255) {
        throw UdfException("The number of arguments of a Python UDF must be between 0 and 255");
    }
    if (returnType == nullptr || returnType->isUndefined()) {
        throw UdfException("A defined return type for a Python UDF must be set");
    }
}

}// namespace NES::Catalogs::UDF