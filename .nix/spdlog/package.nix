{
  lib,
  llvmPackages_19,
  cmake,
  ninja,
  pkg-config,
  fetchFromGitHub,
  fmt_11,
}:

let
  llvm = llvmPackages_19;
  clangStdenv = llvm.stdenv;
  libcxxStdenv = llvm.libcxxStdenv;

  concatFlags = flags: if flags == [ ] then "" else lib.concatStringsSep " " flags;

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
      fmtPkg ? null,
      compilerFlags ? [ ],
      linkerFlags ? [ ],
    }:
    let
      stdenv = if useLibcxx then libcxxStdenv else clangStdenv;
      selectedFmt = if fmtPkg != null then fmtPkg else fmt_11;
      libcxxFlags = lib.optionals useLibcxx [
        "-DCMAKE_CXX_FLAGS=-stdlib=libc++"
        "-DCMAKE_EXE_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_SHARED_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_MODULE_LINKER_FLAGS=-stdlib=libc++"
      ];
    in
    stdenv.mkDerivation rec {
      pname = "spdlog";
      version = "1.15.3";

      src = fetchFromGitHub {
        owner = "gabime";
        repo = "spdlog";
        rev = "v${version}";
        hash = "sha512-FFV/vkIEEaZ5nCNU4akdcvTDWAI+JC3EIkPhS/DTvvoV1O8MvXXzbz7Y75nYl06IV9g+cJlNE/ueC2gxGwp14g==";
      };

      nativeBuildInputs = [
        cmake
        ninja
        pkg-config
      ];
      buildInputs = lib.unique ([ selectedFmt ] ++ extraBuildInputs);

      cmakeFlags = [
        "-G"
        "Ninja"
        "-DSPDLOG_FMT_EXTERNAL=ON"
        "-DSPDLOG_BUILD_TESTS=OFF"
        "-DSPDLOG_BUILD_EXAMPLE=OFF"
        "-DSPDLOG_BUILD_BENCH=OFF"
      ]
      ++ libcxxFlags;

      env = lib.optionalAttrs (compilerFlags != [ ]) {
        NIX_CFLAGS_COMPILE = concatFlags compilerFlags;
      };

      enableParallelBuilding = true;
      strictDeps = true;
      doCheck = false;

      meta = with lib; {
        description = "Very fast, header-only/compiled, C++ logging library";
        homepage = "https://github.com/gabime/spdlog";
        license = licenses.mit;
        platforms = platforms.linux;
      };
    };

  parseWithSanitizerArgs =
    arg:
    if builtins.isList arg then
      {
        extraBuildInputs = arg;
        useLibcxx = false;
        fmtPkg = null;
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
        fmtPkg = arg.fmtPkg or null;
        compilerFlags = arg.compilerFlags or [ ];
        linkerFlags = arg.linkerFlags or [ ];
      }
    else
      {
        extraBuildInputs = [ ];
        useLibcxx = false;
        fmtPkg = null;
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
