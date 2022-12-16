#include <Nautilus/Backends/MLIR/Passes/SerializeToCubinPass.hpp>
namespace NES::Nautilus::Backends::MLIR {

SerializeToCubinPass::SerializeToCubinPass() {
    this->triple = "nvptx64-nvidia-cuda";
    this->chip = "sm_35";
    this->features = "+ptx60";
}

void SerializeToCubinPass::getDependentDialects(mlir::DialectRegistry& registry) const {
    registerNVVMDialectTranslation(registry);
    mlir::gpu::SerializeToBlobPass::getDependentDialects(registry);
}

std::unique_ptr<std::vector<char>> SerializeToCubinPass::serializeISA(const std::string& isa) {
    mlir::Location loc = getOperation().getLoc();
    char jitErrorBuffer[4096] = {0};

    RETURN_ON_CUDA_ERROR(cuInit(0));

    // Linking requires a device context.
    CUdevice device;
    RETURN_ON_CUDA_ERROR(cuDeviceGet(&device, 0));
    CUcontext context;
    RETURN_ON_CUDA_ERROR(cuCtxCreate(&context, 0, device));
    CUlinkState linkState;

    CUjit_option jitOptions[] = {CU_JIT_ERROR_LOG_BUFFER, CU_JIT_ERROR_LOG_BUFFER_SIZE_BYTES};
    void* jitOptionsVals[] = {jitErrorBuffer, reinterpret_cast<void*>(sizeof(jitErrorBuffer))};

    RETURN_ON_CUDA_ERROR(cuLinkCreate(2,              /* number of jit options */
                                      jitOptions,     /* jit options */
                                      jitOptionsVals, /* jit option values */
                                      &linkState));

    auto kernelName = getOperation().getName().str();
    RETURN_ON_CUDA_ERROR(cuLinkAddData(linkState,
                                       CUjitInputType::CU_JIT_INPUT_PTX,
                                       const_cast<void*>(static_cast<const void*>(isa.c_str())),
                                       isa.length(),
                                       kernelName.c_str(),
                                       0,       /* number of jit options */
                                       nullptr, /* jit options */
                                       nullptr  /* jit option values */
                                       ));

    void* cubinData;
    size_t cubinSize;
    RETURN_ON_CUDA_ERROR(cuLinkComplete(linkState, &cubinData, &cubinSize));

    char* cubinAsChar = static_cast<char*>(cubinData);
    auto result = std::make_unique<std::vector<char>>(cubinAsChar, cubinAsChar + cubinSize);

    // This will also destroy the cubin data.
    RETURN_ON_CUDA_ERROR(cuLinkDestroy(linkState));
    RETURN_ON_CUDA_ERROR(cuCtxDestroy(context));

    return result;
}
}// namespace NES::Nautilus::Backends::MLIR
