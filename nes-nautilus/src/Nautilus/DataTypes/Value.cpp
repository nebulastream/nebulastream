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

#include <Nautilus/DataTypes/Value.hpp>

#include <map>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

Value Value::scalar(VarVal v)
{
    std::map<std::string, VarVal> components;
    components.emplace(std::string{SCALAR_KEY}, std::move(v));
    return Value{std::move(components)};
}

Value::Value(std::map<std::string, VarVal> components) : componentsMap(std::move(components))
{
    PRECONDITION(not componentsMap.empty(), "Value must hold at least one component");
}

bool Value::isScalar() const
{
    return componentsMap.size() == 1 && componentsMap.begin()->first == SCALAR_KEY;
}

const VarVal& Value::asScalar() const
{
    PRECONDITION(isScalar(), "Value::asScalar called on a compound Value with {} components", componentsMap.size());
    return componentsMap.begin()->second;
}

const VarVal& Value::component(std::string_view suffix) const
{
    const auto it = componentsMap.find(std::string{suffix});
    INVARIANT(it != componentsMap.end(), "Value has no component with suffix '{}'", suffix);
    return it->second;
}

const std::map<std::string, VarVal>& Value::components() const
{
    return componentsMap;
}

Value Value::castToType(DataType::Type type) const
{
    return Value::scalar(asScalar().castToType(type));
}

nautilus::val<bool> Value::isNull() const
{
    return asScalar().isNull();
}

bool Value::isNullable() const
{
    return asScalar().isNullable();
}

Value::operator bool() const
{
    return static_cast<bool>(asScalar());
}

Value Value::operator+(const Value& other) const
{
    return Value::scalar(asScalar() + other.asScalar());
}

Value Value::operator-(const Value& other) const
{
    return Value::scalar(asScalar() - other.asScalar());
}

Value Value::operator*(const Value& other) const
{
    return Value::scalar(asScalar() * other.asScalar());
}

Value Value::operator/(const Value& other) const
{
    return Value::scalar(asScalar() / other.asScalar());
}

Value Value::operator%(const Value& other) const
{
    return Value::scalar(asScalar() % other.asScalar());
}

Value Value::operator==(const Value& other) const
{
    return Value::scalar(asScalar() == other.asScalar());
}

Value Value::operator!=(const Value& other) const
{
    return Value::scalar(asScalar() != other.asScalar());
}

Value Value::operator&&(const Value& other) const
{
    return Value::scalar(asScalar() && other.asScalar());
}

Value Value::operator||(const Value& other) const
{
    return Value::scalar(asScalar() || other.asScalar());
}

Value Value::operator<(const Value& other) const
{
    return Value::scalar(asScalar() < other.asScalar());
}

Value Value::operator>(const Value& other) const
{
    return Value::scalar(asScalar() > other.asScalar());
}

Value Value::operator<=(const Value& other) const
{
    return Value::scalar(asScalar() <= other.asScalar());
}

Value Value::operator>=(const Value& other) const
{
    return Value::scalar(asScalar() >= other.asScalar());
}

Value Value::operator&(const Value& other) const
{
    return Value::scalar(asScalar() & other.asScalar());
}

Value Value::operator|(const Value& other) const
{
    return Value::scalar(asScalar() | other.asScalar());
}

Value Value::operator^(const Value& other) const
{
    return Value::scalar(asScalar() ^ other.asScalar());
}

Value Value::operator<<(const Value& other) const
{
    return Value::scalar(asScalar() << other.asScalar());
}

Value Value::operator>>(const Value& other) const
{
    return Value::scalar(asScalar() >> other.asScalar());
}

Value Value::operator!() const
{
    return Value::scalar(!asScalar());
}

nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const Value& value)
{
    return os << value.asScalar();
}

}
