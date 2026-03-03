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

#include <Nautilus/DataTypes/LazyValueProvider.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
#include <LazyValueRepresentationRegistry.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES::LazyValueProvider
{
std::shared_ptr<LazyValueRepresentation> provideLazyValueRepresentation(
    const std::string& typeName,
    const nautilus::val<int8_t*>& valueAddress,
    const nautilus::val<uint64_t>& size,
    const DataType::Type& type)
{
    auto args = LazyValueRepresentationRegistryArguments{.valueAddress = valueAddress, .size = size, .type = type};
    if (auto optionalLazyVal = LazyValueRepresentationRegistry::instance().create(typeName, args))
    {
        return optionalLazyVal.value();
    }
    /// The default case is the LazyValueRepresentation class, which overrides all functions
    return std::make_shared<LazyValueRepresentation>(LazyValueRepresentation(valueAddress, size, type));
}
}
