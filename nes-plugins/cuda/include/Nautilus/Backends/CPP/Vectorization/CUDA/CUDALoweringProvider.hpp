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

#ifndef NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_BACKENDS_CPP_VECTORIZATION_CUDA_CUDALOWERINGPROVIDER_HPP_
#define NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_BACKENDS_CPP_VECTORIZATION_CUDA_CUDALOWERINGPROVIDER_HPP_

#include <Nautilus/Backends/CPP/CPPLoweringProvider.hpp>
#include <Nautilus/CodeGen/CodeGenerator.hpp>
#include <Nautilus/CodeGen/Segment.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Types/Stamp.hpp>

namespace NES::Nautilus::Backends::CPP::Vectorization::CUDA {

/*
 * @brief a specialized class to lower operator code to cuda
 */
class CUDALoweringProvider : public CPPLoweringProvider {
  public:
    explicit CUDALoweringProvider();

    constexpr static auto TUPLE_BUFFER_IDENTIFIER = "NES__CUDA__TupleBuffer";
    constexpr static auto SLICE_STORE_IDENTIFIER = "NES__CUDA__SliceStore";

    std::unique_ptr<CodeGen::Segment> lowerGraph(const std::shared_ptr<IR::IRGraph>& irGraph);

  public:
    static std::string getVariable(const std::string& id);
    static std::string getType(const IR::Types::StampPtr& stamp);

    using RegisterFrame = Frame<std::string, std::string>;
    explicit CUDALoweringProvider(const RegisterFrame& registerFrame);

  private:
    std::unique_ptr<CodeGen::CodeGenerator> lowerProxyCall(const std::shared_ptr<IR::Operations::ProxyCallOperation>& operation,
                                                           RegisterFrame& frame);

    std::unique_ptr<CodeGen::Segment> functionBody;
    RegisterFrame registerFrame;
};

}// namespace NES::Nautilus::Backends::CPP::Vectorization::CUDA

#endif// NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_BACKENDS_CPP_VECTORIZATION_CUDA_CUDALOWERINGPROVIDER_HPP_
