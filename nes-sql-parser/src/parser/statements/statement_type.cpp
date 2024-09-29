//
// Created by Usama Bin Tariq on 18.09.24.
//

#include <iostream>
#include <nebula/parser/statements/statement_type.hpp>
#include <string>
#include <vector>
#include <memory>

namespace nebula {
    std::string StatementReturnTypeToString(StatementType type) {
        switch (type) {
            case StatementType::SELECT_STATEMENT:
                return "SELECT";
            case StatementType::INSERT_STATEMENT:
                return "INSERT";
            case StatementType::UPDATE_STATEMENT:
                return "UPDATE";
            case StatementType::DELETE_STATEMENT:
                return "DELETE";
            case StatementType::PREPARE_STATEMENT:
                return "PREPARE";
            case StatementType::EXECUTE_STATEMENT:
                return "EXECUTE";
            case StatementType::ALTER_STATEMENT:
                return "ALTER";
            case StatementType::TRANSACTION_STATEMENT:
                return "TRANSACTION";
            case StatementType::COPY_STATEMENT:
                return "COPY";
            case StatementType::COPY_DATABASE_STATEMENT:
                return "COPY_DATABASE";
            case StatementType::ANALYZE_STATEMENT:
                return "ANALYZE";
            case StatementType::VARIABLE_SET_STATEMENT:
                return "VARIABLE_SET";
            case StatementType::CREATE_FUNC_STATEMENT:
                return "CREATE_FUNC";
            case StatementType::EXPLAIN_STATEMENT:
                return "EXPLAIN";
            case StatementType::CREATE_STATEMENT:
                return "CREATE";
            case StatementType::DROP_STATEMENT:
                return "DROP";
            case StatementType::PRAGMA_STATEMENT:
                return "PRAGMA";
            case StatementType::VACUUM_STATEMENT:
                return "VACUUM";
            case StatementType::RELATION_STATEMENT:
                return "RELATION";
            case StatementType::EXPORT_STATEMENT:
                return "EXPORT";
            case StatementType::CALL_STATEMENT:
                return "CALL";
            case StatementType::SET_STATEMENT:
                return "SET";
            case StatementType::LOAD_STATEMENT:
                return "LOAD";
            case StatementType::EXTENSION_STATEMENT:
                return "EXTENSION";
            case StatementType::LOGICAL_PLAN_STATEMENT:
                return "LOGICAL_PLAN";
            case StatementType::ATTACH_STATEMENT:
                return "ATTACH";
            case StatementType::DETACH_STATEMENT:
                return "DETACH";
            case StatementType::MULTI_STATEMENT:
                return "MULTI";
            case StatementType::UPDATE_EXTENSIONS_STATEMENT:
                return "UPDATE_EXTENSIONS";
            case StatementType::INVALID_STATEMENT:
                break;
        }
        return "INVALID";
    }
}
