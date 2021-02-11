/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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


#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <string.h>
#include <z3++.h>
#include "Exceptions/InvalidQueryException.hpp"

namespace NES {

bool NES::SyntacticQueryValidation::isValid(std::string inputQuery) {
    try{
        QueryPtr multipleFilterQuery = UtilityFunctions::createQueryFromCodeString(inputQuery);
    } catch(const std::exception& ex) {
        handleException(ex);
        return false;
    } catch(...) {
        return false;
    }
    return true;
}

void NES::SyntacticQueryValidation::handleException(const std::exception& ex){

    std::string error_message = ex.what();
    std::string start_str = "error: ";
    std::string end_str = "^";

    int start_idx = error_message.find(start_str) + start_str.length();
    int end_idx = error_message.find(end_str) + end_str.length();
    std::string clean_error_message; 
    
    if(start_idx == -1 || end_idx == -1){
        clean_error_message = error_message;
    } else {
        clean_error_message = error_message.substr(start_idx, end_idx-start_idx); 
    }

    clean_error_message = "SyntacticQueryValidation:\n" + clean_error_message;
    
    throw InvalidQueryException(clean_error_message + "\n");
}

}// namespace NES