#pragma once
#include "base_table_ref.hpp"
#include "table_ref_type.hpp"

namespace nebula {
    class TableRef : public BaseTableRef {
    public:
        static constexpr const TableReferenceType TYPE = TableReferenceType::INVALID;
        virtual ~TableRef() = default;
    };
}