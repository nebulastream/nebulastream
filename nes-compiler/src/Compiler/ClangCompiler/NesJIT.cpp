
#include <Compiler/ClangCompiler/NesJIT.hpp>

#include <filesystem>
#include <iostream>

bool LLVMinit = false;

#define ERROR_MSG(msg) std::cout << "[ERROR]: " << msg << std::endl;
#define DEBUG_MSG(msg) std::cout << "[DEBUG]: " << msg << std::endl;



using namespace llvm;

void InitializeLLVM() {
    if (LLVMinit) {
        return;
    }

    // Run the global LLVM pass initialization functions.
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    InitializeNativeTargetAsmParser();

    auto& Registry = *PassRegistry::getPassRegistry();

    initializeCore(Registry);
    initializeScalarOpts(Registry);
    initializeVectorization(Registry);
    initializeIPO(Registry);
    initializeAnalysis(Registry);
    initializeTransformUtils(Registry);
    initializeInstCombine(Registry);
    initializeInstrumentation(Registry);
    initializeTarget(Registry);

    LLVMinit = true;
}

void check_LLVM_error(Error Err) {
    if (Err) {
        DEBUG_MSG("LLVM error: " << toString(std::move(Err)));
    }
}

template<typename T>
T CHECK_LLVM(Expected<T>&& E) {
    check_LLVM_error(E.takeError());
    return std::move(*E);
}

NESJIT::NESJIT(bool debug) {
    InitializeLLVM();

    diagnosticOptions = new clang::DiagnosticOptions;
    textDiagnosticPrinter = std::make_unique<clang::TextDiagnosticPrinter>(outs(), diagnosticOptions.get());
    // The diagnotic engine should not own the client below (or it could if we release our unique_ptr.)
    diagnosticsEngine = new clang::DiagnosticsEngine(diagIDs, diagnosticOptions, textDiagnosticPrinter.get(), false);
    auto& compilerInvocation = compilerInstance.getInvocation();

    std::stringstream ss;
    ss << "-triple=" << sys::getDefaultTargetTriple();

    std::vector<const char*> itemcstrs;
    std::vector<std::string> itemstrs;
    itemstrs.push_back(ss.str());

    itemstrs.push_back("-x");
    itemstrs.push_back("c++");
    //  itemstrs.push_back("-std=c++17");

    for (unsigned idx = 0; idx < itemstrs.size(); idx++) {
        itemcstrs.push_back(itemstrs[idx].c_str());// do NOT modify itemctrs afterwards (->invalid pointers)
    }

    clang::CompilerInvocation::CreateFromArgs(compilerInvocation, itemcstrs, *diagnosticsEngine);

    auto& preprocessorOptions = compilerInvocation.getPreprocessorOpts();
    auto& targetOptions = compilerInvocation.getTargetOpts();
    auto& frontEndOptions = compilerInvocation.getFrontendOpts();
    auto& headerSearchOptions = compilerInvocation.getHeaderSearchOpts();
    headerSearchOptions.Verbose = false;
    headerSearchOptions.UserEntries.clear();

    addCppStdLibHeaderSearchPaths(headerSearchOptions);

    targetOptions.Triple = sys::getDefaultTargetTriple();

    std::unique_ptr<LLVMContext> context = std::make_unique<LLVMContext>();

    tsContext = std::make_unique<orc::ThreadSafeContext>(std::move(context));
    action = std::make_unique<clang::EmitLLVMOnlyAction>(tsContext->getContext());

    compilerInstance.createDiagnostics(textDiagnosticPrinter.get(), false);
    if (!debug) {
        lljit = CHECK_LLVM(orc::LLJITBuilder().setNumCompileThreads(2).create());
    } else {
        std::cout << "Creating LLVM IR with full debug info."
                  << "\n";
        // Default DebugInfo values is "NoDebugInfo". Setting this value to "FullDebugInfo means that when the Clang frontend
        // invokes the compilerInstance and creates the LLVM IR, it will add all possible DebugInfos to the LLVM IR.
        // This, combined with creating a Gnome Debugger (GDB) registration listener (done in the next section), enables the
        // GDB to map the assembly code via the LLVM IR(with debug info) back to the original source code
        compilerInstance.getCodeGenOpts().setDebugInfo(clang::codegenoptions::FullDebugInfo);

        // Create LLJIT instance that uses a custom ObjectLinkingLayer
        // The custom OLL registers the GDBRegistrationListener with the RTDyldObjectLinkingLayer
        // By doing this, the GDB is given an entry point for the JIT'd code
        ExitOnError ExitOnErr;
        auto JTMB = ExitOnErr(llvm::orc::JITTargetMachineBuilder::detectHost());
        lljit = CHECK_LLVM(orc::LLJITBuilder()
                               .setNumCompileThreads(2)
                               .setJITTargetMachineBuilder(std::move(JTMB))
                               .setObjectLinkingLayerCreator([&](llvm::orc::ExecutionSession& ES, const Triple&) {
                                   auto GetMemMgr = []() {
                                       return std::make_unique<SectionMemoryManager>();
                                   };
                                   auto ObjLinkingLayer =
                                       std::make_unique<llvm::orc::RTDyldObjectLinkingLayer>(ES, std::move(GetMemMgr));

                                   // Register the event listener.
                                   ObjLinkingLayer->registerJITEventListener(*JITEventListener::createGDBRegistrationListener());

                                   // Make sure the debug info sections aren't stripped.
                                   ObjLinkingLayer->setProcessAllSections(true);

                                   return ObjLinkingLayer;
                               })
                               .create());
    }
    // The LLJITWithGDBRegistrationListener tutorial had extra arguments for ".getGlobalPrefix
    // if an issue arises that results from symbols that can not be found, this spot might be worth investigating
    lljit->getMainJITDylib().addGenerator(
        cantFail(orc::DynamicLibrarySearchGenerator::GetForCurrentProcess(lljit->getDataLayout().getGlobalPrefix())));
}

void NESJIT::optimizationPass(uint8_t optimizationType, clang::CompilerInvocation compilerInvocation) {
    if (optimizationType > 0) {
        std::cout << "DEBUG: Using optimization of type: " << static_cast<unsigned int>(optimizationType)
                  << ". Type 1 debug info may be asynchronously printed after the function finished!" << '\n';
        auto& codeGenOptions = compilerInvocation.getCodeGenOpts();
        LoopAnalysisManager loopAnalysisManager;
        FunctionAnalysisManager functionAnalysisManager;
        CGSCCAnalysisManager cGSCCAnalysisManager;
        ModuleAnalysisManager moduleAnalysisManager;
        PassBuilder passBuilder;
        ModulePassManager modulePassManager =
            passBuilder.buildPerModuleDefaultPipeline(PassBuilder::OptimizationLevel::O3,
                                                      (static_cast<unsigned int>(optimizationType) == 1));

        passBuilder.registerModuleAnalyses(moduleAnalysisManager);
        passBuilder.registerCGSCCAnalyses(cGSCCAnalysisManager);
        passBuilder.registerFunctionAnalyses(functionAnalysisManager);
        passBuilder.registerLoopAnalyses(loopAnalysisManager);
        passBuilder.crossRegisterProxies(loopAnalysisManager,
                                         functionAnalysisManager,
                                         cGSCCAnalysisManager,
                                         moduleAnalysisManager);

        modulePassManager.run(*module, moduleAnalysisManager);
    }
}

void NESJIT::addCppStdLibHeaderSearchPaths(clang::HeaderSearchOptions& headerSearchOptions) {
    bool searchPathsFile = std::filesystem::exists("../include/cppStdLibHeaderSearchPaths.h");

    if (searchPathsFile) {
        //    std::cout << "Using cppStdLibHeaderSearchPaths.h for header search paths." << '\n';
        //addCPPStdLibHeaderSearchPaths(headerSearchOptions);
    } else {
        std::cout << "Could not find include/cppStdLibHeaderSearchPaths.h Using static paths." << '\n';
        headerSearchOptions.AddPath("/usr/include/c++/11/", clang::frontend::System, false, false);
        headerSearchOptions.AddPath("/usr/include/x86_64-linux-gnu/c++/11/", clang::frontend::System, false, false);
        headerSearchOptions.AddPath("/usr/include/", clang::frontend::System, false, false);
        headerSearchOptions.AddPath("/usr/include/x86_64-linux-gnu/", clang::frontend::System, false, false);
        headerSearchOptions.AddPath("/usr/lib/gcc/x86_64-linux-gnu/11/include/", clang::frontend::System, false, false);
    }
}

void NESJIT::loadModule(const std::string& functionName, uint8_t optimizationType) {
    if (!compilerInstance.ExecuteAction(*action)) {
        ERROR_MSG("Cannot execute action with compiler instance.");
    }

    module = action->takeModule();
    if (!module) {
        ERROR_MSG("Cannot retrieve IR module.");
    }

    // List the functions in the module before optimizations:
    //  DEBUG_MSG("Before optimization module function list: ");
    int t = 0;
    for (auto& f : *module) {
        //    std::cout << "func" << t++ << ":\n" << f.getName().str() << '\n';

        std::basic_string<char> res = demangle(f.getName().str());
        if (!res.empty()) {
            //      std::cout << "Function demangled name is:\n" << res << "\n\n";
            if (!res.find(functionName, 0)) {
                demangledFunctionName = res;
            }

            // And we map that entry in the function names:
            functionNames.insert(
                std::make_pair<std::string, std::string>(static_cast<std::basic_string<char>&&>(res), f.getName().str()));
        }
    }
    optimizationPass(optimizationType, compilerInstance.getInvocation());
    auto err = lljit->addIRModule(orc::ThreadSafeModule(std::move(module), *tsContext));

    check_LLVM_error(std::move(err));
}

void NESJIT::loadModuleFromFile(const std::string& file, const std::string& functionName, uint8_t optimizationType) {
    auto& compilerInvocation = compilerInstance.getInvocation();
    auto& frontEndOptions = compilerInvocation.getFrontendOpts();

    frontEndOptions.Inputs.clear();
    frontEndOptions.Inputs.push_back(clang::FrontendInputFile(StringRef(file), clang::InputKind(clang::Language::CXX)));

    loadModule(functionName, optimizationType);
}

void NESJIT::loadModuleFromBuffer(const std::string& buffer, const std::string& functionName, uint8_t optimizationType) {
    auto& compilerInvocation = compilerInstance.getInvocation();
    auto& frontEndOptions = compilerInvocation.getFrontendOpts();

    frontEndOptions.Inputs.clear();
    std::unique_ptr<MemoryBuffer> buf = llvm::MemoryBuffer::getMemBuffer(llvm::StringRef(buffer.data(), buffer.size()));
    frontEndOptions.Inputs.push_back(clang::FrontendInputFile(buf->getBuffer(), clang::InputKind(clang::Language::CXX)));

    loadModule(functionName, optimizationType);
}

JITTargetAddress NESJIT::lookup() {
    auto it = functionNames.find(demangledFunctionName);
    std::string fname;
    if (it != functionNames.end()) {
        fname = it->second;
        //    DEBUG_MSG("using mangled name: " << fname);
    } else {
        fname = demangledFunctionName;
    }
    JITEvaluatedSymbol sym = CHECK_LLVM(lljit->lookupLinkerMangled(fname));
    return sym.getAddress();
}