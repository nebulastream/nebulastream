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
#include <memory>
#include <unordered_map>
#include <vector>


namespace NES
{
namespace Configuration
{
class SingleNodeWorkerConfiguration;
} /// namespace Configuration

namespace QueryCompilation
{
class QueryCompilerOptions;
using QueryCompilerOptionsPtr = std::shared_ptr<QueryCompilerOptions>;
} /// namespace QueryCompilation

namespace QueryCompilation::Phases
{

class PhaseFactory
{
public:
};
using PhaseFactoryPtr = std::shared_ptr<PhaseFactory>;

class DefaultPhaseFactory : public PhaseFactory
{
public:
    virtual ~DefaultPhaseFactory() = default;
    static PhaseFactoryPtr create();
};

} /// namespace NES::QueryCompilation::Phases


namespace Runtime
{

class NodeEngine
{
    std::vector<std::shared_ptr<int>> bufferManagers;
    std::unordered_map<int, std::shared_ptr<int>> registeredQueries;
};

using NodeEnginePtr = std::shared_ptr<NodeEngine>;

} /// namespace NES::Runtime

namespace Runtime
{
class NodeEngineBuilder
{
public:
    std::unique_ptr<NodeEngine> build();
};
} /// namespace NES::Runtime


namespace QueryCompilation
{

class QueryCompiler
{
public:
    virtual std::shared_ptr<int> compileQuery(std::shared_ptr<int> request) = 0;
    virtual ~QueryCompiler() = default;

protected:
};

class NautilusQueryCompiler : public QueryCompilation::QueryCompiler
{
public:
    std::shared_ptr<int> compileQuery(std::shared_ptr<int> request) override;

    NautilusQueryCompiler(QueryCompilation::QueryCompilerOptionsPtr const& options, Phases::PhaseFactoryPtr const& phaseFactory);
};

} /// namespace NES::QueryCompilation


class SingleNodeWorker
{
    std::unique_ptr<QueryCompilation::QueryCompiler> qc;
    std::shared_ptr<Runtime::NodeEngine> nodeEngine;

public:
    explicit SingleNodeWorker(const Configuration::SingleNodeWorkerConfiguration&);
    ~SingleNodeWorker();
    SingleNodeWorker(const SingleNodeWorker& other) = delete;
    SingleNodeWorker& operator=(const SingleNodeWorker& other) = delete;
    SingleNodeWorker(SingleNodeWorker&& other) noexcept;
    SingleNodeWorker& operator=(SingleNodeWorker&& other) noexcept;
};

static QueryCompilation::QueryCompilerOptionsPtr createQueryCompilationOptions()
{
    return {};
}

SingleNodeWorker::~SingleNodeWorker() = default;
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const Configuration::SingleNodeWorkerConfiguration& configuration)
    : qc(std::make_unique<QueryCompilation::NautilusQueryCompiler>(
          createQueryCompilationOptions(), QueryCompilation::Phases::DefaultPhaseFactory::create()))
    , nodeEngine(Runtime::NodeEngineBuilder().build())
{
}

}

int main()
{
    return 0;
}
