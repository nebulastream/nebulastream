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

#include <Util/Common.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

bool Boolean::equals(DataTypePtr otherDataType)
{
    return NES::Util::instanceOf<Boolean>(otherDataType);
}

DataTypePtr Boolean::join(DataTypePtr otherDataType)
{
    if (NES::Util::instanceOf<Boolean>(otherDataType))
    {
        return DataTypeFactory::createBoolean();
    }
    return DataTypeFactory::createUndefined();
}

std::string Boolean::toString()
{
    return "Boolean";
}

} /// namespace NES
