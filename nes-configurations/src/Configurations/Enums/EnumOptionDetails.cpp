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

#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Configurations/Enums/QueryCompilerType.hpp>
#include <Configurations/Enums/CompilationStrategy.hpp>
#include <Configurations/Enums/PipeliningStrategy.hpp>
#include <Configurations/Enums/WindowingStrategy.hpp>
#include <Configurations/Enums/OutputBufferOptimizationLevel.hpp>
#include <Configurations/Enums/MemoryLayoutPolicy.hpp>
#include <Configurations/Enums/QueryExecutionMode.hpp>
#include <Configurations/Enums/QueryMergerRule.hpp>
#include <Configurations/Enums/EnumOptionDetails.hpp>
#include <Configurations/Enums/DumpMode.hpp>
#include <Configurations/Enums/NautilusBackend.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Configurations {

template class EnumOption<NES::Spatial::Experimental::SpatialType>;
template class EnumOption<NES::QueryCompilation::QueryCompilerType>;
template class EnumOption<NES::QueryCompilation::CompilationStrategy>;
template class EnumOption<NES::QueryCompilation::PipeliningStrategy>;
template class EnumOption<NES::QueryCompilation::DumpMode>;
template class EnumOption<NES::QueryCompilation::NautilusBackend>;
template class EnumOption<NES::QueryCompilation::OutputBufferOptimizationLevel>;
template class EnumOption<NES::QueryCompilation::WindowingStrategy>;
template class EnumOption<NES::Runtime::QueryExecutionMode>;
template class EnumOption<NES::Spatial::Mobility::Experimental::LocationProviderType>;
template class EnumOption<NES::Optimizer::QueryMergerRule>;
template class EnumOption<NES::LogLevel>;
template class EnumOption<NES::Optimizer::MemoryLayoutPolicy>;

}// namespace NES::Configurations