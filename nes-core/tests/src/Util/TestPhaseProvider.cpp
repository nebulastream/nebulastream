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
#include <Util/TestPhaseProvider.hpp>
#include <Util/TestSinkProvider.hpp>
#include <Util/TestSourceProvider.hpp>

namespace NES::TestUtils {
QueryCompilation::LowerToExecutableQueryPlanPhasePtr
TestPhaseProvider::createLowerToExecutableQueryPlanPhase(QueryCompilation::QueryCompilerOptionsPtr options, bool) {
    auto sinkProvider = std::make_shared<TestUtils::TestSinkProvider>();
    auto sourceProvider = std::make_shared<TestUtils::TestSourceProvider>(options);
    return QueryCompilation::LowerToExecutableQueryPlanPhase::create(sinkProvider, sourceProvider);
}
}// namespace NES::TestUtils