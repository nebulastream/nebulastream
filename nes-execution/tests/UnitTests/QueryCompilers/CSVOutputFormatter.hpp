//
// Created by ls on 12/1/24.
//

#pragma once
#include "MemoryRecordAccessor.hpp"
#include "TupleBufferRef.hpp"
#include "VarVal.hpp"

namespace NES
{
class CSVOutputFormatter : MemoryOutputAccessor
{
    nautilus::val<bool> append(TupleBufferRef output, std::vector<Nautilus::VarVal> fields) override;
    PhysicalSchema schema;
};
}