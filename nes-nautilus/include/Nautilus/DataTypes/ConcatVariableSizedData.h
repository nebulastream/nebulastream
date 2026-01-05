#pragma once
#include "Arena.hpp"
#include "VariableSizedData.hpp"

namespace NES
{

class ConcatVariableSizedData : public VariableSizedData
{
public:
    ConcatVariableSizedData(VariableSizedData first, VariableSizedData second, ArenaRef& arena);
    [[nodiscard]] nautilus::val<int8_t*> getContent();
    [[nodiscard]] nautilus::val<bool> isValid() const;
    nautilus::val<bool> operator==(const VariableSizedData&) const;

private:
    void materialize();

    ArenaRef& arenaRef;
    nautilus::val<int8_t*> materializedContent;

    VariableSizedData first;
    VariableSizedData second;
};

};