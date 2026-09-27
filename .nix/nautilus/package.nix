{
  pkgs,
  mlirBinary,
}:

let
  lib = pkgs.lib;
  llvm = pkgs.llvmPackages_19;
  clangStdenv = llvm.stdenv;
  libcxxStdenv = llvm.libcxxStdenv;

  fmtPkg = pkgs.fmt_11;
  spdlogPkg = pkgs.spdlog.override { fmt = fmtPkg; };

  nautilusSrc = pkgs.fetchFromGitHub {
    owner = "nebulastream";
    repo = "nautilus";
    rev = "3a49d8eaf40a34aadf3e76023cd209027f7a247e";
    hash = "sha256-/U2uPDl7qKKP2QELvJ4wZk3W46BRNik/rMafvGGilvE=";
  };

  baseBuildInputs = [
    mlirBinary
    llvm.llvm
    fmtPkg
    spdlogPkg
    pkgs.binutils
    pkgs.elfutils
    pkgs.libdwarf.dev
    pkgs.zlib.dev
    pkgs.libffi
    pkgs.libxml2
    pkgs.boost
    pkgs.openssl.dev
    pkgs.tbb
    pkgs.python3
  ];

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
    }:
    let
      selectedStdenv = if useLibcxx then libcxxStdenv else clangStdenv;
    in
    selectedStdenv.mkDerivation rec {
      pname = "nautilus";
      version = "0.1";

      src = nautilusSrc;

      nativeBuildInputs = [
        pkgs.cmake
        pkgs.ninja
        pkgs.pkg-config
      ];
      buildInputs = baseBuildInputs ++ extraBuildInputs;

      CMAKE_CXX_FLAGS = "-Wno-error=unused-result";

      cmakeFlags = [
        "-G"
        "Ninja"
        "-DCMAKE_BUILD_TYPE=Release"
        "-DUSE_EXTERNAL_FMT=ON"
        "-DUSE_EXTERNAL_SPDLOG=ON"
        "-DUSE_EXTERNAL_MLIR=ON"
        "-DNAUTILUS_DOWNLOAD_MLIR=OFF"
        "-DENABLE_LOGGING=ON"
        "-DENABLE_STACKTRACE=OFF"
        "-DENABLE_COMPILER=ON"
        "-DENABLE_TRACING=ON"
        "-DENABLE_MLIR_BACKEND=ON"
        "-DENABLE_C_BACKEND=ON"
        "-DENABLE_BC_BACKEND=OFF"
        "-DENABLE_ASMJIT_BACKEND=ON"
        "-DENABLE_TBC_BACKEND=ON"
        "-DENABLE_TBC_JIT=ON"
        "-DENABLE_BUILTIN_PLUGIN=ON"
        "-DENABLE_PROFILING_PLUGIN=ON"
        "-DENABLE_SIMD_PLUGIN=ON"
        "-DENABLE_STD_PLUGIN=ON"
        "-DENABLE_SPECIALIZATION_PLUGIN=ON"
        "-DENABLE_INLINING_PLUGIN=ON"
        "-DENABLE_GPU_PLUGIN=OFF"
        "-DENABLE_TESTS=OFF"
        "-DMLIR_DIR=${mlirBinary}/lib/cmake/mlir"
        "-DLLVM_DIR=${mlirBinary}/lib/cmake/llvm"
      ];

      enableParallelBuilding = true;
      strictDeps = true;

      meta = with lib; {
        description = "NebulaStream Nautilus compiler";
        homepage = "https://github.com/nebulastream/nautilus";
        license = licenses.asl20;
        platforms = platforms.linux;
      };
    };

in
{
  default = build { };
  withSanitizer =
    arg:
    let
      extraBuildInputs = if builtins.isList arg then arg else (arg.extraBuildInputs or [ ]);
      useLibcxx = if builtins.isAttrs arg then (arg.useLibcxx or false) else false;
    in
    build { inherit extraBuildInputs useLibcxx; };
}
