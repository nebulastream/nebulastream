#ifndef NES_SERIALIZETOCUBINPASS_HPP
#define NES_SERIALIZETOCUBINPASS_HPP

#include <mlir/Dialect/GPU/Passes.h>
#include <mlir/Pass/Pass.h>
#include <mlir/Target/LLVMIR/Dialect/NVVM/NVVMToLLVMIRTranslation.h>
#include <llvm/Support/TargetSelect.h>
#include <cuda.h>

namespace NES::Nautilus::Backends::MLIR {
class SerializeToCubinPass : public mlir::PassWrapper<SerializeToCubinPass, mlir::gpu::SerializeToBlobPass> {
  public:
    //    MLIR_DEFINE_EXPLICIT_INTERNAL_INLINE_TYPE_ID(TestSerializeToCubinPass)

    mlir::StringRef getArgument() const final { return "test-gpu-to-cubin"; }
    mlir::StringRef getDescription() const final { return "Lower GPU kernel function to CUBIN binary annotations"; }
    SerializeToCubinPass();
    static void emitCudaError(const llvm::Twine& expr, const char* buffer, CUresult result, mlir::Location loc) {
        const char* error;
        cuGetErrorString(result, &error);
        emitError(loc, expr.concat(" failed with error code ").concat(llvm::Twine{error}).concat("[").concat(buffer).concat("]"));
    }

  private:
    void getDependentDialects(mlir::DialectRegistry& registry) const override;

    // Serializes PTX to CUBIN.
    std::unique_ptr<std::vector<char>> serializeISA(const std::string& isa) override;
};

#define RETURN_ON_CUDA_ERROR(expr)                                                                                               \
    do {                                                                                                                         \
        if (auto status = (expr)) {                                                                                              \
            emitCudaError(#expr, jitErrorBuffer, status, loc);                                                                   \
            return {};                                                                                                           \
        }                                                                                                                        \
    } while (false)

}// namespace NES::Nautilus::Backends::MLIR

#endif//NES_SERIALIZETOCUBINPASS_HPP
