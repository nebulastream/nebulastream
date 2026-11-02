# NES Codon plugin SDK

This component defines the stable descriptor used by Codon UDF extension shared libraries and the host-side registry that loads them.

An independently built extension can use the installed CMake package:

```cmake
find_package(NESCodonPlugin CONFIG REQUIRED)
find_package(OpenCV CONFIG REQUIRED)

add_codon_extension_plugin(my-codon-opencv
    PLUGIN_NAME opencv
    ADDITIONAL_SOURCES src/OpenCv.cpp
    CODON_PATH "${CMAKE_CURRENT_SOURCE_DIR}/codon"
    NATIVE_SYMBOLS my_cv_imdecode my_cv_resize
    NATIVE_SYMBOL_HEADERS MyOpenCvFunctions.hpp
    LINK_LIBRARIES opencv_core opencv_imgcodecs opencv_imgproc)
```

Every `.codon` and `.py` file below `CODON_PATH` is embedded under its relative path. The generated descriptor advertises those modules and the named native C functions. Native functions may import the `seq_*` Codon host ABI; in particular, results whose lifetime is owned by a UDF invocation should use `seq_alloc`.

`CODON_PATH` and `ADDITIONAL_SOURCES` are optional. A native symbol implemented by a linked dependency, such as OpenBLAS, only needs `NATIVE_SYMBOLS`, `NATIVE_SYMBOL_HEADERS`, and `LINK_LIBRARIES`.

Native symbols require declarations from the headers passed through `NATIVE_SYMBOL_HEADERS`. Only the descriptor entry point is exported from the shared object; native function addresses are carried in the descriptor and remain local to the plugin.

The extension does not link against NebulaStream. At runtime, NES loads its descriptor with `dlopen(RTLD_LOCAL)`, exposes its embedded modules to Codon's resource filesystem, admits its declared functions during LLVM-module validation, and explicitly registers their descriptor-provided addresses with the pipeline JIT.
