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

#include <Nautilus/Interface/Formatting/BinaryRowEncoder.hpp>

#include <cstdint>
#include <cstring>

#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <function.hpp>
#include <val_ptr.hpp>
#include "DataTypes/Schema.hpp"

namespace NES::Nautilus
{

BinaryRowEncoder::BinaryRowEncoder(const Schema& schema)
{
    fields.reserve(schema.getNumberOfFields());
    for (const auto& f : schema.getFields())
    {
        uint32_t fsz = 0;
        switch (f.dataType.type)
        {
            case DataType::Type::VARSIZED:
                fsz = 0;
                break;
            default:
                fsz = f.dataType.getSizeInBytes();
                break;
        }
        fields.push_back(FieldInfo{.name = f.name, .type = f.dataType, .fixedSize = fsz});
    }
}

nautilus::val<uint32_t> BinaryRowEncoder::computeSize(Record& record) const
{
    using namespace nautilus;
    val<uint32_t> total = 0U;
    for (const auto& fi : fields)
    {
        if (fi.fixedSize > 0)
        {
            total = total + static_cast<uint32_t>(fi.fixedSize);
        }
        else /// Varsized
        {
            const auto& vv = record.read(fi.name);
            auto sizeVar = vv.customVisit(
                []<typename T>(T&& underlying)
                {
                    if constexpr (std::is_same_v<T, VariableSizedData>)
                    {
                        return underlying.getSize();
                    }
                    else
                    {
                        return nautilus::val<uint32_t>(0U);
                    }
                });
            auto dynSize = sizeVar.cast<nautilus::val<uint32_t>>();
            total = total + dynSize;
        }
    }

    return total;
}

void BinaryRowEncoder::encodeTo(Record& record, const nautilus::val<int8_t*>& dst) const
{
    using namespace nautilus;
    auto cur = dst;

    for (const auto& fi : fields)
    {
        if (fi.fixedSize > 0)
        {
            const auto& vv = record.read(fi.name);
            vv.writeToMemory(cur);
            cur = cur + static_cast<uint64_t>(fi.fixedSize);
        }
        else
        {
            const auto& vv = record.read(fi.name);
            vv.writeToMemory(cur);
            auto contentLen = invoke(
                +[](int8_t* p)
                {
                    uint32_t n = 0;
                    std::memcpy(&n, p, sizeof(uint32_t));
                    return n;
                },
                cur);
            auto dynSize = contentLen + nautilus::val<uint32_t>(static_cast<uint32_t>(sizeof(uint32_t)));
            const nautilus::val<uint64_t> dynSize64 = dynSize;
            cur = cur + dynSize64;
        }
    }
}

}
