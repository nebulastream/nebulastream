//
// Created by ls on 26.09.23.
//

#ifndef NES_LLVMMODULESTRIPPER_H
#define NES_LLVMMODULESTRIPPER_H
namespace llvm {
class Module;
}

class LLVMModuleStripper {
  public:
    void dance(llvm::Module& module);
};

#endif//NES_LLVMMODULESTRIPPER_H
