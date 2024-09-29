#pragma once
#include "parsed_expression.hpp"
#include <string>
#include <vector>


namespace nebula {
    class ColumnExpression : public ParsedExpression {
    public:
        ColumnExpression();

        //! Specify both the column and table name
        ColumnExpression(std::string column_name, std::string table_name);

        //! Only specify the column name, the table name will be derived later
        explicit ColumnExpression(std::string column_name);

        //! Specify a set of names
        explicit ColumnExpression(std::vector<std::string> column_names);

        //! The stack of names in order of which they appear (column_names[0].column_names[1].column_names[2]....)
        std::vector<std::string> column_names = {};

        std::string ToString() override;
    };
}
