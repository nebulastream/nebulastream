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

#pragma once
#include <cstdint>

#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES
{

template <typename Val>
class StaticVector
{
    std::vector<Val> vec;

public:
    explicit StaticVector(const std::vector<Val>& vec) : vec(vec) { }

    nautilus::val<Val> operator[](nautilus::val<size_t> key)
    {
        for (nautilus::static_val<size_t> i = 0; i < this->vec.size(); ++i)
        {
            if (i == key)
            {
                return this->vec[i];
            }
        }
        throw std::out_of_range("out of range");
    }
};

class MemoryProvider
{
public:
    struct Tuple
    {
        enum Type : uint8_t
        {
            INT32,
            UINT32,
            INT8,
        };

        std::vector<std::size_t> fieldOffsets;
        std::vector<Type> fieldTypes;
        nautilus::val<int8_t*> buffer;


        Nautilus::VarVal load(nautilus::val<int8_t*> buffer, Type type)
        {
            switch (type)
            {
                case INT32:
                    return nautilus::val<int32_t>(*static_cast<nautilus::val<int32_t*>>(buffer));
                case UINT32:
                    return nautilus::val<uint32_t>(*static_cast<nautilus::val<uint32_t*>>(buffer));
                case INT8:
                    return nautilus::val<int8_t>(*static_cast<nautilus::val<int8_t*>>(buffer));
            }

            throw std::out_of_range("MemoryProvider::getFieldType");
        }

        Nautilus::VarVal load(nautilus::val<size_t> index)
        {
            if (nautilus::static_val<size_t> i = 0; i < fieldOffsets.size())
            {
                if (index == i)
                {
                    return load(buffer + fieldOffsets[i], fieldTypes[i]);
                }
            }
            return Nautilus::VarVal(3);
        }

        Nautilus::VarVal operator[](nautilus::val<size_t> index) { return load(index); };
    };

    nautilus::val<size_t> indexOfTuple(nautilus::val<size_t> index) { return index * tupleSize; }

    Tuple operator[](nautilus::val<size_t> index)
    {
        return Tuple{{0, 4, 8}, {Tuple::Type::INT32, Tuple::Type::INT32, Tuple::Type::INT32}, buffer + indexOfTuple(index)};
    }

    MemoryProvider(const nautilus::val<int8_t*>& buffer) : tupleSize(12), buffer(buffer) { }

private:
    size_t tupleSize;
    nautilus::val<int8_t*> buffer;
};
}