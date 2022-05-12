// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at

//         https://www.apache.org/licenses/LICENSE-2.0

//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// */

// #ifndef NES_EXTERNALFUNCTIONOPERATION_HPP
// #define NES_EXTERNALFUNCTIONOPERATION_HPP

// #include <Experimental/NESIR/Operations/Operation.hpp>
// #include <vector>

// namespace NES {

// class ExternalFunctionOperation : public Operation {
//   public:
//     ExternalFunctionOperation(ExternalFunctionType externalFunctionType, std::vector<OperationPtr> functionArgs);
//     ~ExternalFunctionOperation() override = default;

// ExternalFunctionOperation getExternalFunctionType();
//     std::vector<OperationPtr> getFunctionArgs();
//     bool classof(const Operation* Op);

//   private:
//     std::string name; //Would be needed to create a prototype for a new external function
//     ExternalFunctionType externalFunctionType; //Would be needed to access MemberFunctionOperations correctly 
//                -> actually no! could check name (e.g. if getDataBuffer) in Generating Operation. If so, access MemberFunctions at correct spot -> hard coded
//     std::vector<OperationPtr> functionArgs; // Would be needed for both
//     std::vector<Operation::BasicType> argTypes; //Would be needed to create a prototype for a new external function
// };
// }// namespace NES
// #endif//NES_EXTERNALFUNCTIONOPERATION_HPP
