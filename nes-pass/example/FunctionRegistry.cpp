//
// Created by benni on 7/8/25.
//

#include "FunctionRegistry.h"

FunctionRegistry& FunctionRegistry::instance() {
    static FunctionRegistry inst;
    return inst;
}

int FunctionRegistry::addToRegistry(FuncPtr fn, std::string bitcode) {
    std::lock_guard<std::mutex> lock(mutex_);
    registry_[fn] = bitcode;
    return 0;
}

std::string FunctionRegistry::getMetadata(FuncPtr fn) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = registry_.find(fn);
    return it != registry_.end() ? it->second : std::string();
}


int registerFunction(FunctionRegistry::FuncPtr fn, const char *bitcodePtr, uint64_t bitcodeLen)
{
    return FunctionRegistry::instance().addToRegistry(fn, std::string(bitcodePtr, bitcodeLen));
}



