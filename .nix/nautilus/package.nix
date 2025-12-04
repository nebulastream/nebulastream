{ pkgs }:

let
  lib = pkgs.lib;
  llvm = pkgs.llvmPackages_19;
  clangStdenv = llvm.stdenv;

  mlirBinary = pkgs.stdenvNoCC.mkDerivation rec {
    pname = "nes-mlir";
    version = "20";

    src =
      if pkgs.stdenv.hostPlatform.isAarch64 then
        pkgs.fetchurl {
          url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-20/nes-llvm-20-arm64-none-libstdcxx.tar.zstd";
          sha256 = "3e74c024e865efac646abe09b2cadcf88530b9af799b2d6fd0a5e5e1e01e685e";
        }
      else if pkgs.stdenv.hostPlatform.isx86_64 then
        pkgs.fetchurl {
          url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-20/nes-llvm-20-x64-none-libstdcxx.tar.zstd";
          sha256 = "aef98d5bd61a8530392796e86a15a36d7fde9d553579cf64dfd465454e3fae7a";
        }
      else
        throw "Unsupported system: ${pkgs.stdenv.hostPlatform.system}";

    nativeBuildInputs = [
      pkgs.zstd
      pkgs.gnutar
    ];

    dontUnpack = true;
    strictDeps = true;

    installPhase = ''
      runHook preInstall
      mkdir -p "$TMPDIR/mlir"
      zstd -d "$src" --stdout | tar -xf - -C "$TMPDIR/mlir"
      mkdir -p "$out"
      cp -r "$TMPDIR/mlir"/clang/* "$out"/
      runHook postInstall
    '';

    meta = with lib; {
      description = "Prebuilt MLIR toolchain used by NebulaStream";
      license = licenses.asl20;
      platforms = platforms.linux;
    };
  };

  fmtPkg = pkgs.fmt_11;
  spdlogPkg = pkgs.spdlog.override { fmt = fmtPkg; };

  nautilusSrc = pkgs.fetchFromGitHub {
    owner = "nebulastream";
    repo = "nautilus";
    rev = "48ec95391c9f984dfcc24a63aefe43d38e9d7b1d";
    hash = "sha512-r+BOalsNn3qUum4GWEb+RD8dOtBgx7LlJx8yTLLq7W/A2SRvZP1u3uJ/bgFFaqCeXp+weXLkdgz6EG5D5sAL4Q==";
  };

  nautilus = clangStdenv.mkDerivation rec {
    pname = "nautilus";
    version = "0.1";

    src = nautilusSrc;
    patches = [
      ./patches/0001-disable-ubsan-function-call-check.patch
      ./patches/0003-ubsan-fix-variadic-expansion.patch
    ];

    nativeBuildInputs = [
      pkgs.cmake
      pkgs.ninja
      pkgs.pkg-config
    ];
    buildInputs = [
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
      "-DENABLE_INLINING_PASS=OFF"
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
  inherit mlirBinary nautilus;
}
