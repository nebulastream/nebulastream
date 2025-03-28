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

#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Phases/NautilusCompilationPhase.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include "Phases/AddScanAndEmitPhase.hpp"
#include "Phases/LowerToExecutableQueryPlanPhase.hpp"
#include "Phases/PipeliningPhase.hpp"

namespace NES::QueryCompilation
{

// TODO: The request should include a configuration
QueryCompiler::QueryCompiler(std::shared_ptr<QueryCompilerConfiguration> options) : options(options)
{
}

std::unique_ptr<ExecutableQueryPlan> QueryCompiler::compileQuery(std::shared_ptr<QueryCompilationRequest>, QueryId)
{
    try
    {
        // TODO: the query id does not need to part of all the operators here?
        /// For now we have to override the id here as it should not be set by the client
        //request->decomposedQueryPlan->setQueryId(queryId);

        /// 0) here we should allow logical reorderings. These rules are simpler to construct.

        /// 1) get the logical query plan
        // auto nodeEngine = request->nodeEngine;
        // auto bufferSize = nodeEngine->getQueryManager()->getBufferManager()->getBufferSize();
        // auto logicalQueryPlan = request->decomposedQueryPlan;

        /// During lowering we add the operator handlers?
        /// 2) lower to physical query plan (old Nautilus)
        // auto physicalQueryPlan = LowerLogicalToNautilusOperators::apply(logicalQueryPlan);

        /// When lowering the scan operators cannot be used as they are not 'Operators'.
        /// The current way we mode it is, that a pipeline is a 'Operator' which then can be placed in the DQP.
        /// We have a decomposed query plan with pipelines as 'operators' (clever!)
        ///
        /// The problem is that we have now a type missmatch, scan etc cannot fit.

        /// 3) custom transformation rules
        /// We do not require that the functions are pure or deterministaic, e.i., they can internally use statistics.
        /// TODO here we can register other transformation rules

        // TODO should add scan and emit where this is needed
        /// 4) Pipelining & Scan and emit
        //auto pipelinedQueryPlan = PipeliningPhase::apply(physicalQueryPlan);

        ///pipelinedQueryPlan = AddScanAndEmitPhase::apply(physicalQueryPlan, bufferSize);

        /// 5) Here we create a executable pipeline state (compilation or interpretation)
        /// They are included into executable operators
        /// TODO I think, we can also already start the compilation here
        // TODO shouldn't the executable operator not be called ExecutablePipeline?
        ///pipelinedQueryPlan = NautilusCompilationPhase::apply(pipelinedQueryPlan, options);

        /// 6) create a executable query plan
        return std::unique_ptr<ExecutableQueryPlan>();//LowerToExecutableQueryPlanPhase::apply(PipelinedQueryPlan(), request->nodeEngine);
    }
    catch (...)
    {
        // TODO: add proper error handling here
        tryLogCurrentException();
        return {};
    }
}
}
