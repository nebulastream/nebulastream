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

namespace NES {

bool NES::SyntacticQueryValidation::isValid(std::string inputQuery) {

    try{
        QueryPtr multipleFilterQuery = UtilityFunctions::createQueryFromCodeString(inputQuery);
    } catch(const std::exception& ex) {
        std::cout << "EXCEPTION THROWN BY QUERY VALIDATOR (exception)" << std::endl;
        std::cout << ex.what() << std::endl;
        return false;
    } catch(...) {
        std::cout << "We landed in generic catch";
        return false;
    }
    return true;
}

}// namespace NES