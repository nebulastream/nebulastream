//
// Created by Maxim Popov on 05.01.26.
//

#include "../../../include/Nautilus/DataTypes/ConcatVariableSizedData.h"

#include <std/cstring.h>

namespace NES
{

ConcatVariableSizedData::ConcatVariableSizedData(VariableSizedData first, VariableSizedData second, ArenaRef& arena)
    : VariableSizedData(nullptr, first.getSize() + second.getSize()), arenaRef(arena), first(first), second(second)
{
}

nautilus::val<int8_t*> ConcatVariableSizedData::getContent()
{
    materialize();
    return materializedContent;
}

void ConcatVariableSizedData::materialize()
{
    ptrToVarSized = arenaRef.allocateMemory(size);

    /// Writing the left value and then the right value to the new variable sized data
    nautilus::memcpy(ptrToVarSized, first.getContent(), second.getSize());
    nautilus::memcpy(ptrToVarSized + first.getSize(), second.getContent(), second.getSize());
}

nautilus::val<bool> ConcatVariableSizedData::isValid() const
{
    return first.isValid() && second.isValid();
}

nautilus::val<bool> ConcatVariableSizedData::operator==(const VariableSizedData& rhs) const
{
    if (getSize() != rhs.getSize())
    {
        return {false};
    }
    // For now the comparison of the two concat values will be suboptimal, as getContent will trigger materialization
    // We first assume that first and second values are regular varsized, not concat ones
    // TODO: optimize case when first and second are concat values
    const auto firstVarSizedData = first.getContent();
    const auto secondVarSizedData = second.getContent();
    const auto rhsVarSizedData = rhs.getContent();

    const auto compareResult1 = (nautilus::memcmp(firstVarSizedData, rhsVarSizedData, first.getSize()) == 0);
    const auto compareResult2 = (nautilus::memcmp(secondVarSizedData, rhsVarSizedData + first.getSize(), second.getSize()) == 0);
    return {compareResult1 && compareResult2};
}

}
