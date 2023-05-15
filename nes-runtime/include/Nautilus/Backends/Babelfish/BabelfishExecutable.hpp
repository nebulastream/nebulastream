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

#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_EXECUTABLE_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_EXECUTABLE_HPP_
#include "Nautilus/Backends/Babelfish/BabelfishCode.hpp"
#include <Compiler/DynamicObject.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <jni.h>
#include <memory>
namespace NES::Nautilus::Backends::Babelfish {

/**
 * @brief Implements the executable for the cpp backend
 */
class BabelfishExecutable : public Executable {
  public:
    /**
     * Constructor to create a cpp executable.
     * @param obj the shared object, which we invoke at runtime.
     */
    explicit BabelfishExecutable(BabelfishCode code, JNIEnv* env, jclass entripointClazz, jobject pipelineObject);
    ~BabelfishExecutable()  override ;

  public:
    void* getInvocableFunctionPtr(const std::string& member) override;
    bool hasInvocableFunctionPtr() override;
    std::unique_ptr<GenericInvocable> getGenericInvocable(const std::string& string) override;

  private:
    BabelfishCode code;
    JNIEnv* env;
    jclass entripointClazz;
    jobject pipelineObject;
};
}// namespace NES::Nautilus::Backends::Babelfish

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BABELFISH_EXECUTABLE_HPP_
