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

#include "Util/Timer.hpp"
#include <Nautilus/Backends/MLIR/LLVMIROptimizer.hpp>
#include <iostream>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/Mangling.h>
#include <llvm/Support/CommandLine.h>
#include <llvm/Support/FileCollector.h>
#include <llvm/Transforms/IPO/SCCP.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>
#include <mlir/ExecutionEngine/OptUtils.h>
#include <mlir/Target/LLVMIR/Dialect/LLVMIR/LLVMToLLVMIRTranslation.h>
#include <fstream>


namespace NES::Nautilus::Backends::MLIR {

llvm::function_ref<llvm::Error(llvm::Module*)> 
LLVMIROptimizer::getLLVMOptimizerPipeline(OptimizationLevel optLevel, bool inlining) {    
    // Return LLVM optimizer pipeline.
    if(inlining) {
        switch(optLevel) {
            case OptimizationLevel::O0: {
                return [] (llvm::Module* llvmIRModule) mutable {
                    auto timer = std::make_shared<Timer<>>("LLVM IR Module Optimization");
                    timer->start();
                    llvm::SMDiagnostic Err;
                    std::cout << "lambda dir: " << std::string(PROXY_FUNCTIONS_RESULT_DIR2) << '\n';
                    std::cout << "Next line\n";
                    // std::string 
                    auto proxyFunctionsIR = llvm::parseIRFile(std::string(PROXY_FUNCTIONS_RESULT_DIR2),
                                                            Err, llvmIRModule->getContext());
                    timer->snapshot("Proxy Module Parsed");
                    llvm::Linker::linkModules(*llvmIRModule, std::move(proxyFunctionsIR));
                    timer->snapshot("Proxy Module Linked");

                    auto optPipeline = mlir::makeOptimizingTransformer(0, 3, nullptr);
                    auto optimizedModule = optPipeline(llvmIRModule);
                    timer->snapshot("Linked Module Optimized");

                    timer->pause();
                    std::string timerString;
                    for(auto snapshot : timer->getSnapshots()) {
                        timerString += std::to_string(snapshot.getPrintTime()) + ',';
                    }
                    timerString += '\n';
                    // Write inlining results to folder where benchmark is executed (will be removed later).
                    std::ofstream fs("llvmLambda.csv", std::ios::app);
                    if(fs.is_open()) { 
                        fs.write(timerString.c_str(), timerString.size());
                    }

                    std::string llvmIRString;
                    llvm::raw_string_ostream llvmStringStream(llvmIRString);
                    llvmIRModule->print(llvmStringStream, nullptr);

                    auto* basicError = new std::error_code();
                    llvm::raw_fd_ostream fileStream(std::string(PROXY_FUNCTIONS_RESULT_DIR2) + "generated.ll", *basicError);
                    fileStream.write(llvmIRString.c_str(), llvmIRString.length());
                    return optimizedModule;
                };
            }
            case OptimizationLevel::O1: {
                return [] (llvm::Module* llvmIRModule) mutable {
                    auto timer = std::make_shared<Timer<>>("LLVM IR Module Optimization");
                    timer->start();
                    llvm::SMDiagnostic Err;
                    std::cout << "lambda dir: " << std::string(PROXY_FUNCTIONS_RESULT_DIR2) << '\n';
                    std::cout << "Next line\n";
                    // std::string 
                    auto proxyFunctionsIR = llvm::parseIRFile(std::string(PROXY_FUNCTIONS_RESULT_DIR2),
                                                            Err, llvmIRModule->getContext());
                    timer->snapshot("Proxy Module Parsed");
                    llvm::Linker::linkModules(*llvmIRModule, std::move(proxyFunctionsIR));
                    timer->snapshot("Proxy Module Linked");

                    auto optPipeline = mlir::makeOptimizingTransformer(1, 3, nullptr);
                    auto optimizedModule = optPipeline(llvmIRModule);
                    timer->snapshot("Linked Module Optimized");

                    timer->pause();
                    std::string timerString;
                    for(auto snapshot : timer->getSnapshots()) {
                        timerString += std::to_string(snapshot.getPrintTime()) + ',';
                    }
                    timerString += '\n';
                    // Write inlining results to folder where benchmark is executed (will be removed later).
                    std::ofstream fs("llvmLambda.csv", std::ios::app);
                    if(fs.is_open()) { 
                        fs.write(timerString.c_str(), timerString.size());
                    }

                    std::string llvmIRString;
                    llvm::raw_string_ostream llvmStringStream(llvmIRString);
                    llvmIRModule->print(llvmStringStream, nullptr);

                    auto* basicError = new std::error_code();
                    llvm::raw_fd_ostream fileStream(std::string(PROXY_FUNCTIONS_RESULT_DIR2) + "generated.ll", *basicError);
                    fileStream.write(llvmIRString.c_str(), llvmIRString.length());
                    return optimizedModule;
                };
            }
            case OptimizationLevel::O2: {
                return [] (llvm::Module* llvmIRModule) mutable {
                    auto timer = std::make_shared<Timer<>>("LLVM IR Module Optimization");
                    timer->start();
                    llvm::SMDiagnostic Err;
                    std::cout << "lambda dir: " << std::string(PROXY_FUNCTIONS_RESULT_DIR2) << '\n';
                    std::cout << "Next line\n";
                    // std::string 
                    auto proxyFunctionsIR = llvm::parseIRFile(std::string(PROXY_FUNCTIONS_RESULT_DIR2),
                                                            Err, llvmIRModule->getContext());
                    timer->snapshot("Proxy Module Parsed");
                    llvm::Linker::linkModules(*llvmIRModule, std::move(proxyFunctionsIR));
                    timer->snapshot("Proxy Module Linked");

                    std::cout << "OPT LEVEL 2\n";
                    auto optPipeline = mlir::makeOptimizingTransformer(2, 3, nullptr);
                    auto optimizedModule = optPipeline(llvmIRModule);
                    timer->snapshot("Linked Module Optimized");

                    timer->pause();
                    std::string timerString;
                    for(auto snapshot : timer->getSnapshots()) {
                        timerString += std::to_string(snapshot.getPrintTime()) + ',';
                    }
                    timerString += '\n';
                    // Write inlining results to folder where benchmark is executed (will be removed later).
                    std::ofstream fs("llvmLambda.csv", std::ios::app);
                    if(fs.is_open()) { 
                        fs.write(timerString.c_str(), timerString.size());
                    }

                    std::string llvmIRString;
                    llvm::raw_string_ostream llvmStringStream(llvmIRString);
                    llvmIRModule->print(llvmStringStream, nullptr);

                    auto* basicError = new std::error_code();
                    llvm::raw_fd_ostream fileStream(std::string(PROXY_FUNCTIONS_RESULT_DIR2) + "generated.ll", *basicError);
                    fileStream.write(llvmIRString.c_str(), llvmIRString.length());
                    return optimizedModule;
                };
            }
            case OptimizationLevel::O3: {
                return [] (llvm::Module* llvmIRModule) mutable {
                    auto timer = std::make_shared<Timer<>>("LLVM IR Module Optimization");
                    timer->start();
                    llvm::SMDiagnostic Err;
                    std::cout << "lambda dir: " << std::string(PROXY_FUNCTIONS_RESULT_DIR2) << '\n';
                    std::cout << "Next line\n";
                    // std::string 
                    auto proxyFunctionsIR = llvm::parseIRFile(std::string(PROXY_FUNCTIONS_RESULT_DIR2),
                                                            Err, llvmIRModule->getContext());
                    timer->snapshot("Proxy Module Parsed");
                    llvm::Linker::linkModules(*llvmIRModule, std::move(proxyFunctionsIR));
                    timer->snapshot("Proxy Module Linked");

                    std::cout << "OPT LEVEL 3\n";
                    auto optPipeline = mlir::makeOptimizingTransformer(3, 3, nullptr);
                    auto optimizedModule = optPipeline(llvmIRModule);
                    timer->snapshot("Linked Module Optimized");

                    timer->pause();
                    std::string timerString;
                    for(auto snapshot : timer->getSnapshots()) {
                        timerString += std::to_string(snapshot.getPrintTime()) + ',';
                    }
                    timerString += '\n';
                    // Write inlining results to folder where benchmark is executed (will be removed later).
                    std::ofstream fs("llvmLambda.csv", std::ios::app);
                    if(fs.is_open()) { 
                        fs.write(timerString.c_str(), timerString.size());
                    }

                    std::string llvmIRString;
                    llvm::raw_string_ostream llvmStringStream(llvmIRString);
                    llvmIRModule->print(llvmStringStream, nullptr);

                    auto* basicError = new std::error_code();
                    llvm::raw_fd_ostream fileStream(std::string(PROXY_FUNCTIONS_RESULT_DIR2) + "generated.ll", *basicError);
                    fileStream.write(llvmIRString.c_str(), llvmIRString.length());
                    return optimizedModule;
                };
            }
        }
    } else {
        switch(optLevel) {
            case OptimizationLevel::O0: {
                return [] (llvm::Module* llvmIRModule) mutable {
                    auto optPipeline = mlir::makeOptimizingTransformer(0, 0, nullptr);
                    auto optimizedModule = optPipeline(llvmIRModule);
                    // llvmIRModule->print(llvm::outs(), nullptr);
                    return optimizedModule;
                };
            }
            case OptimizationLevel::O1: {
                return [] (llvm::Module* llvmIRModule) mutable {
                    auto optPipeline = mlir::makeOptimizingTransformer(1, 1, nullptr);
                    auto optimizedModule = optPipeline(llvmIRModule);
                    // llvmIRModule->print(llvm::outs(), nullptr);
                    return optimizedModule;
                };
            }
            case OptimizationLevel::O2: {
                return [] (llvm::Module* llvmIRModule) mutable {
                    auto optPipeline = mlir::makeOptimizingTransformer(2, 2, nullptr);
                    auto optimizedModule = optPipeline(llvmIRModule);
                    // llvmIRModule->print(llvm::outs(), nullptr);
                    return optimizedModule;
                };
            }
            case OptimizationLevel::O3: {
                return [] (llvm::Module* llvmIRModule) mutable {
                    auto optPipeline = mlir::makeOptimizingTransformer(3, 3, nullptr);
                    auto optimizedModule = optPipeline(llvmIRModule);
                    // llvmIRModule->print(llvm::outs(), nullptr);
                    return optimizedModule;
                };
            }
        }
    }
}
}// namespace NES::Nautilus::Backends::MLIR