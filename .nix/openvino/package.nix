{
  lib,
  llvmPackages_19,
  fetchurl,
  cmake,
  ninja,
  pkg-config,
  python3,
  nlohmann_json,
  pugixml,
  tbb,
  xbyak,
}:

let
  llvm = llvmPackages_19;
  clangStdenv = llvm.stdenv;
  libcxxStdenv = llvm.libcxxStdenv;

  openvinoSrc = fetchurl {
    url = "https://github.com/openvinotoolkit/openvino/archive/refs/tags/2025.3.0.tar.gz";
    hash = "sha256-lpobiBrQED3VIrWwlzhDQmHRFY67I6ywAOr+9VJo974=";
  };

  oneDnnCpuSrc = fetchurl {
    url = "https://github.com/openvinotoolkit/oneDNN/archive/3d7a6f1d068d8ae08f189aa4baa93d177bc07507.tar.gz";
    hash = "sha256-NU+pxEpv6uoGIwCC4bn1PqhTg2op5AMDILqOSANXYUs=";
  };

  mlasSrc = fetchurl {
    url = "https://github.com/openvinotoolkit/mlas/archive/d1bc25ec4660cddd87804fcf03b2411b5dfb2e94.tar.gz";
    hash = "sha256-CkT7/UsT6GCdZt2sSxGifJDBB0zeUkTJGtGXkBZmAEw=";
  };

  concatFlags = flags: if flags == [ ] then "" else lib.concatStringsSep " " flags;

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
      nlohmannJsonPkg ? nlohmann_json,
      pugixmlPkg ? pugixml,
      tbbPkg ? tbb,
      xbyakPkg ? xbyak,
      compilerFlags ? [ ],
      linkerFlags ? [ ],
    }:
    let
      stdenv = if useLibcxx then libcxxStdenv else clangStdenv;
      cmakeCxxFlags = compilerFlags ++ lib.optionals useLibcxx [ "-stdlib=libc++" ];
      cmakeLinkerFlags = linkerFlags ++ lib.optionals useLibcxx [ "-stdlib=libc++" ];
      sanitizerCmakeFlags =
        lib.optionals (cmakeCxxFlags != [ ])
          [ "-DCMAKE_CXX_FLAGS=${concatFlags cmakeCxxFlags}" ]
        ++ lib.optionals (cmakeLinkerFlags != [ ])
          [
            "-DCMAKE_EXE_LINKER_FLAGS=${concatFlags cmakeLinkerFlags}"
            "-DCMAKE_SHARED_LINKER_FLAGS=${concatFlags cmakeLinkerFlags}"
            "-DCMAKE_MODULE_LINKER_FLAGS=${concatFlags cmakeLinkerFlags}"
          ];
    in
    stdenv.mkDerivation rec {
      pname = "openvino";
      version = "2025.3.0";

      src = openvinoSrc;

      patches = [
        ../../vcpkg/vcpkg-registry/ports/openvino/onednn_gpu_includes.patch
        ../../vcpkg/vcpkg-registry/ports/openvino/protobuf-6.patch
        ../../vcpkg/vcpkg-registry/ports/openvino/fix-clang19-stringify.patch
        ../../vcpkg/vcpkg-registry/ports/openvino/fix-libcxx.patch
        ../../vcpkg/vcpkg-registry/ports/openvino/fix-types-libcxx.patch
      ];

      postPatch = ''
        rm -rf src/plugins/intel_cpu/thirdparty/onednn
        mkdir -p src/plugins/intel_cpu/thirdparty/onednn
        tar -xzf ${oneDnnCpuSrc} --strip-components=1 -C src/plugins/intel_cpu/thirdparty/onednn

        rm -rf src/plugins/intel_cpu/thirdparty/mlas
        mkdir -p src/plugins/intel_cpu/thirdparty/mlas
        tar -xzf ${mlasSrc} --strip-components=1 -C src/plugins/intel_cpu/thirdparty/mlas
      '';

      nativeBuildInputs = [
        cmake
        ninja
        pkg-config
        python3
      ];

      buildInputs = lib.unique (
        [
          nlohmannJsonPkg
          pugixmlPkg
          tbbPkg
          xbyakPkg
        ]
        ++ extraBuildInputs
      );

      propagatedBuildInputs = [
        nlohmannJsonPkg
        pugixmlPkg
        tbbPkg
        xbyakPkg
      ];

      cmakeFlags = [
        "-G"
        "Ninja"
        "-DCMAKE_BUILD_TYPE=Release"
        "-DCMAKE_DISABLE_FIND_PACKAGE_OpenCV=ON"
        "-DCPACK_GENERATOR=NIX"
        "-DENABLE_AUTO=OFF"
        "-DENABLE_AUTO_BATCH=OFF"
        "-DENABLE_CLANG_FORMAT=OFF"
        "-DENABLE_CPPLINT=OFF"
        "-DENABLE_DOCS=OFF"
        "-DENABLE_FUNCTIONAL_TESTS=OFF"
        "-DENABLE_HETERO=OFF"
        "-DENABLE_INTEL_CPU=ON"
        "-DENABLE_INTEL_GPU=OFF"
        "-DENABLE_INTEL_NPU=OFF"
        "-DENABLE_JS=OFF"
        "-DENABLE_MLAS_FOR_CPU=ON"
        "-DENABLE_MULTI=ON"
        "-DENABLE_NCC_STYLE=OFF"
        "-DENABLE_OV_IR_FRONTEND=ON"
        "-DENABLE_OV_JAX_FRONTEND=OFF"
        "-DENABLE_OV_ONNX_FRONTEND=OFF"
        "-DENABLE_OV_PADDLE_FRONTEND=OFF"
        "-DENABLE_OV_PYTORCH_FRONTEND=OFF"
        "-DENABLE_OV_TF_FRONTEND=OFF"
        "-DENABLE_OV_TF_LITE_FRONTEND=OFF"
        "-DENABLE_PYTHON=OFF"
        "-DENABLE_SAMPLES=OFF"
        "-DENABLE_SYSTEM_PUGIXML=ON"
        "-DENABLE_SYSTEM_TBB=ON"
        "-DENABLE_TBBBIND_2_5=OFF"
        "-DENABLE_TEMPLATE=OFF"
        "-DENABLE_TESTS=OFF"
        "-DBUILD_SHARED_LIBS=OFF"
        "-DCMAKE_INSTALL_INCLUDEDIR=include"
        "-DCMAKE_INSTALL_LIBDIR=lib"
      ]
      ++ sanitizerCmakeFlags;

      enableParallelBuilding = true;
      strictDeps = true;
      doCheck = false;

      meta = with lib; {
        description = "OpenVINO runtime with CPU plugin and IR frontend";
        homepage = "https://github.com/openvinotoolkit/openvino";
        license = licenses.asl20;
        platforms = platforms.linux;
      };
    };

  parseWithSanitizerArgs =
    arg:
    if builtins.isList arg then
      {
        extraBuildInputs = arg;
        useLibcxx = false;
        nlohmannJsonPkg = nlohmann_json;
        pugixmlPkg = pugixml;
        tbbPkg = tbb;
        xbyakPkg = xbyak;
        compilerFlags = [ ];
        linkerFlags = [ ];
      }
    else if builtins.isAttrs arg then
      {
        extraBuildInputs =
          if arg ? extraBuildInputs then
            arg.extraBuildInputs
          else if arg ? extraPackages then
            arg.extraPackages
          else
            [ ];
        useLibcxx = arg.useLibcxx or false;
        nlohmannJsonPkg = arg.nlohmannJsonPkg or nlohmann_json;
        pugixmlPkg = arg.pugixmlPkg or pugixml;
        tbbPkg = arg.tbbPkg or tbb;
        xbyakPkg = arg.xbyakPkg or xbyak;
        compilerFlags = arg.compilerFlags or [ ];
        linkerFlags = arg.linkerFlags or [ ];
      }
    else
      {
        extraBuildInputs = [ ];
        useLibcxx = false;
        nlohmannJsonPkg = nlohmann_json;
        pugixmlPkg = pugixml;
        tbbPkg = tbb;
        xbyakPkg = xbyak;
        compilerFlags = [ ];
        linkerFlags = [ ];
      };

in
{
  default = build { };
  withSanitizer =
    arg:
    let
      cfg = parseWithSanitizerArgs arg;
    in
    build cfg;
}
