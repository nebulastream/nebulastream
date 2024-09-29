//
// Created by Usama Bin Tariq on 16.09.24.
//

#include <iostream>
#include "nebula/parser/statements/select_statement.hpp"
#include <string>
#include <vector>
#include <memory>

namespace nebula {
    constexpr const StatementType nebula::SelectStatement::TYPE;

    void SelectStatement::Print() const {
        std::cout << "=====Select Statement=====" << std::endl;
        std::cout << "From: ";

        // for (const auto &fr: *from) {
        //     std::cout << fr << ", ";
        // }
        // std::cout << std::endl << "Columns: ";
        // for (const auto &c: *columns) {
        //     std::cout << c << ", ";
        // }
        // std::cout << std::endl;
        // std::cout << "=====END=====" << std::endl << std::endl;
    }

    std::string SelectStatement::ToString() const {
        return std::string("Select");
    }

    bool SelectStatement::VerifyStreamQuery() const {
        return true;
    }

    std::string SelectStatement::ToStreamQuery() const {
        // if (from->empty()) {
        //     return "No From Clause";
        // }
        // //from by , seprated
        // std::string from_joined = "";
        // int count = 0;
        // for (const auto &fr: *from) {
        //     from_joined += fr + (from->size() == (++count) ? "" : ",");
        // }
        //
        // return "Query::from(\"" + from_joined + "\")";
        return "";
    }
}
