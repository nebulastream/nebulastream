//
// Created by ls on 26.09.23.
//

#include <LLVMModuleStripper.hpp>
#include <Util/Logger/Logger.hpp>
#include <llvm/IR/Constants.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Value.h>
#include <llvm/Transforms/IPO/ExtractGV.h>

bool predicate_decls(const llvm::Function& fn) {

    if (!fn.isDeclaration())
        return true;

    bool has_user = false;
    llvm::outs() << fn.getName() << " is used by:\n";
    for (const auto& user : fn.users()) {
        user->print(llvm::outs());
        has_user = true;
    }

    return has_user;
}

bool hacky_predicate(const llvm::Function& fn) {
    if (fn.isDeclaration())
        return true;

    if (fn.getName() == "__cxx_global_var_init" || fn.getName() == "_GLOBAL__sub_I__")
        return true;

    if (fn.getName().starts_with("_ZN3NES9Unikernel5StageILm"))
        return true;

    //NES_DEBUG("Filtering {}", fn.getName());
    return false;
}

#define UNIKERNEL_FUNCTION_TAG 723468
void LLVMModuleStripper::dance(llvm::Module& module) {
    using namespace llvm;

    std::vector<GlobalValue*> toBeDeleted;

    for (auto& fn : module.functions()) {
        if (!hacky_predicate(fn)) {
            toBeDeleted.push_back(&fn);
        }
    }

    for (const auto& gv : toBeDeleted) {
        gv->replaceAllUsesWith(llvm::UndefValue::get(gv->getType()));
        gv->eraseFromParent();
    }

    std::vector<GlobalValue*> removeDeclarations;

    //   for (auto& fn : module.functions()) {
    //        if (!predicate_decls(fn)) {
    //            removeDeclarations.push_back(&fn);
    //        }
    //   }

    //   for (const auto& gv : removeDeclarations) {
    //        gv->replaceAllUsesWith(llvm::UndefValue::get(gv->getType()));
    //        gv->eraseFromParent();
    //   }
    //
    //   auto module.getGlobalVariable(".str",true)->eraseFromParent();
}