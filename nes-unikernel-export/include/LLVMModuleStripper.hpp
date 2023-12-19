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

#ifndef NES_LLVMMODULESTRIPPER_HPP
#define NES_LLVMMODULESTRIPPER_HPP
namespace llvm {
class Module;
}

class LLVMModuleStripper {
  public:
    void dance(llvm::Module& module);
};

#endif//NES_LLVMMODULESTRIPPER_HPP
