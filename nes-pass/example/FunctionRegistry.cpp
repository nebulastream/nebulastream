//
// Created by benni on 7/8/25.
//

#include "FunctionRegistry.h"

InlineFunctionRegistry& InlineFunctionRegistry::instance() {
    static InlineFunctionRegistry inst;
    return inst;
}

int InlineFunctionRegistry::addToRegistry(void *fn, std::string bitcode) {
    std::lock_guard<std::mutex> lock(mutex_);
    registry_[fn] = bitcode;
    return 0;
}

std::string InlineFunctionRegistry::getMetadata(void *fn) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = registry_.find(fn);
    return it != registry_.end() ? it->second : std::string();
}


int registerFunction(void *fn, const char *bitcodePtr, uint64_t bitcodeLen)
{
    return InlineFunctionRegistry::instance().addToRegistry(fn, std::string(bitcodePtr, bitcodeLen));
}


