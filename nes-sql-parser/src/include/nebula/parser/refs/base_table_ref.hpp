#pragma once
#include <string>
#include "table_ref_type.hpp"

namespace nebula {
    class BaseTableRef {
    public:
        static constexpr const TableReferenceType TYPE = TableReferenceType::BASE_TABLE;

        virtual ~BaseTableRef() = default;
        std::string table_name;
    };
}