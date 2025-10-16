{ pkgs
, mlirForSanitizer
, mlirDefault
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

  sanitizeArgs = sanitizer: {
    extraBuildInputs = sanitizer.extraPackages or [ ];
    sanitizeCompileFlags = sanitizer.compileFlags or [ ];
    sanitizeLinkFlags = sanitizer.linkFlags or [ ];
    mlirBinary = mlirForSanitizer sanitizer;
  };

  baseBuildInputs = [
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

  build = {
    extraBuildInputs ? [ ],
    sanitizeCompileFlags ? [ ],
    sanitizeLinkFlags ? [ ],
    mlirBinary ? mlirDefault
  }:
    clangStdenv.mkDerivation rec {
      pname = "nautilus";
      version = "0.1";

      src = nautilusSrc;
      nativeBuildInputs = [
        pkgs.cmake
        pkgs.ninja
        pkgs.pkg-config
      ];
      buildInputs = baseBuildInputs ++ [ mlirBinary ] ++ extraBuildInputs;

      patches = [
        ./patches/mlir-inliner-guard.patch
      ];

      sanitizedCompileFlags =
        lib.concatStringsSep " " (sanitizeCompileFlags ++ [ "-DLLVM_LIFETIME_BOUND=" ]);
      sanitizedLinkFlags = lib.concatStringsSep " " sanitizeLinkFlags;

      NIX_CFLAGS_COMPILE = lib.optionalString (sanitizedCompileFlags != "") sanitizedCompileFlags;
      NIX_CXXFLAGS_COMPILE = lib.optionalString (sanitizedCompileFlags != "") sanitizedCompileFlags;
      NIX_CFLAGS_LINK = lib.optionalString (sanitizedLinkFlags != "") sanitizedLinkFlags;
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
  default = build { mlirBinary = mlirDefault; };
  withSanitizer = sanitizer: build (sanitizeArgs sanitizer);
}
