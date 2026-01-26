{ pkgs }:

let
  lib = pkgs.lib;
  llvm = pkgs.llvmPackages_19;
  clangStdenv = llvm.stdenv;

  mlirBinary = pkgs.stdenvNoCC.mkDerivation rec {
    pname = "nes-mlir";
    version = "21";

    src =
      if pkgs.stdenv.hostPlatform.isAarch64 then
        pkgs.fetchurl {
          url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-21-with-fix-173075/nes-llvm-21-with-fix-173075-arm64-none-libstdcxx.tar.zstd";
          sha256 = "d01f375f5943d25cecf109ceae78797a13ed48214ceddac4d3c8d608d74694bc";
        }
      else if pkgs.stdenv.hostPlatform.isx86_64 then
        pkgs.fetchurl {
          url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-21-with-fix-173075/nes-llvm-21-with-fix-173075-x64-none-libstdcxx.tar.zstd";
          sha256 = "217c4004dab06cc00416733017bdf9f9fc23795c48942cbb08303ac9c0d696a1";
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
    rev = "d75a6816436c824090c85191a0c1e39239cf2479";
    hash = "sha256-M0OOYKk/bCegCv2RpuvJ4Q9gSccpW78r3C9xtrGIHe0=";
  };

  nautilus = clangStdenv.mkDerivation rec {
    pname = "nautilus";
    version = "0.1";

    src = nautilusSrc;
    patches = [
      ./patches/0001-disable-ubsan-function-call-check.patch
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
      "-DENABLE_INLINING_PASS=OFF"
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
  inherit mlirBinary nautilus;
}
