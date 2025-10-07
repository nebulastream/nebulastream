{ pkgs
, mlirBinary
}:

let
  lib = pkgs.lib;
  llvm = pkgs.llvmPackages_19;
  clangStdenv = llvm.stdenv;

  fmtPkg = pkgs.fmt_11;
  spdlogPkg = pkgs.spdlog.override { fmt = fmtPkg; };

  nautilusSrc = pkgs.fetchFromGitHub {
    owner = "nebulastream";
    repo = "nautilus";
    rev = "598041b59644088ed431bb27a35e77f48c1e8bad";
    hash = "sha512-/UXpeKTGNk+fd7ecSPP2RIJ8/hA7E602JnSi4K+s15YKYbKN+LaHSuAXgT3NaJqe3LqYD6NqaFUqMVGmf/6Q3g==";
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

  build = extraBuildInputs:
    clangStdenv.mkDerivation rec {
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

      postPatch = ''
        substituteInPlace nautilus/src/nautilus/compiler/backends/mlir/MLIRCompilationBackend.cpp \
          --replace "context.allowsUnregisteredDialects();" "(void)context.allowsUnregisteredDialects();"
      '';

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

in {
  default = build [ ];
  withSanitizer = build;
}
