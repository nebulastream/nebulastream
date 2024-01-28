/*
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

/*
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

#ifndef OPERATORHANDLERCPPEXPORT_HPP
#define OPERATORHANDLERCPPEXPORT_HPP
#include <OperatorHandlerTracer.hpp>
#include <Stage/QueryPipeliner.hpp>

namespace NES::Unikernel::Export {

class OperatorHandlerCppExporter {
    class HandlerExport {
        std::stringstream ss;
        NES::QuerySubPlanId subPlanId;

      public:
        explicit HandlerExport(NES::QuerySubPlanId subPlanId) : subPlanId(subPlanId) {}

        const char* OP_HANDLER = "NES::Runtime::Execution::OperatorHandler";

        void runtimeIncludes(const std::vector<std::string>& includePaths);

        static std::string typeName(const Runtime::Unikernel::OperatorHandlerParameterDescriptor& parameter);
        void generateParameter(const Runtime::Unikernel::OperatorHandlerParameterDescriptor& parameter);

        void generateHandlerInstantiation(size_t index, const Runtime::Unikernel::OperatorHandlerDescriptor& handler);

        void generateLocalHandlerFunction(const Stage& stage);

        void generateSharedHandlerInstantiation(
            const std::unordered_map<SharedOperatorHandlerIndex, Runtime::Unikernel::OperatorHandlerDescriptor>& sharedHandler);

        [[nodiscard]] std::string str() const { return ss.str(); }
        void externSharedHandlerDefintion();
        void sharedHandlerDefintion();
    };

  public:
    static std::string generateHandler(const Stage& stage, NES::QuerySubPlanId subPlanId);

    static std::string generateSharedHandler(
        const std::unordered_map<SharedOperatorHandlerIndex, Runtime::Unikernel::OperatorHandlerDescriptor>& sharedHandler,
        NES::QuerySubPlanId subPlanId);
};

}// namespace NES::Unikernel::Export

#endif//OPERATORHANDLERCPPEXPORT_HPP
