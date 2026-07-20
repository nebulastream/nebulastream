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

#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypeRegistry.hpp>

/// 3x3 Matrix of FLOAT64 represented as nested fixedsized types. Its purpose is to test the nested fixedsized implementation. Should later be replaced with
/// adhoc matrix creation in the schema definition like Array[3][3]
namespace NES::DataTypeGeneratedRegistrar
{

DataTypeRegistryReturnType RegisterMatrixDataType(DataTypeRegistryArguments args)
{
    const DataType floatArray{DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE), 3};
    const DataType floatMatrix{DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, floatArray, 3};
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("matrix", floatMatrix);
    return DataType{DataType::Type::STRUCT, args.nullable, std::string{"Matrix"}, std::move(fields)};
}
}