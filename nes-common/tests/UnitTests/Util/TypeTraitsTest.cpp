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

#include <Util/TypeTraits.hpp>

#include <expected>
#include <optional>
#include <string>

/// Since TypeTraits only compile static type utilities, this file only has to compile
namespace NES
{
static_assert(!UniqueTypes<int, int, int>);
static_assert(UniqueTypes<int, int&>, "By default cv ref qualification should create distinct types");
static_assert(!UniqueTypesIgnoringCVRef<int, int&>, "IgnoringCVRef should not allow cv ref qualified types");

struct CustomType
{
};

using TypeAlias = CustomType;

struct CustomType1
{
};

static_assert(!UniqueTypes<CustomType, TypeAlias>, "type alias do not create distinct types");
static_assert(UniqueTypes<CustomType, CustomType1>, "\"equal\" types are distinct types");
static_assert(UniqueTypes<CustomType, struct CustomType2>, "\"equal\" types are distinct types");

static_assert(!IsOptional<int>::value);
static_assert(IsOptional<std::optional<int>>::value);
static_assert(!IsOptional<std::optional<int>&>::value, "should not ignore cv qualifier");

static_assert(!Optional<int>);
static_assert(Optional<std::optional<std::optional<int>>>);
static_assert(Optional<std::optional<std::string>>);
static_assert(!Optional<std::expected<std::string, void>>);

}
