#pragma once
#include <val.hpp>

namespace NES
{

class AbstractVariableSizedData
{
public:
    [[nodiscard]] virtual nautilus::val<uint32_t> getSize() const = 0;
    [[nodiscard]] virtual nautilus::val<int8_t*> getContent() const = 0;
    virtual ~AbstractVariableSizedData() = default;
};

};