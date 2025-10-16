{ lib
, llvmPackages_19
, cmake
, ninja
, pkg-config
, fetchFromGitHub
}:

let
  llvmPackages = llvmPackages_19;
  clangStdenv = llvmPackages.stdenv;
  libcxxStdenv = llvmPackages.libcxxStdenv;

  build = { extraBuildInputs ? [], useLibcxx ? false }:
    let
      libcxxFlags = lib.optionals useLibcxx [
        "-DCMAKE_CXX_FLAGS=-stdlib=libc++"
        "-DCMAKE_EXE_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_SHARED_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_MODULE_LINKER_FLAGS=-stdlib=libc++"
      ];
    in
    (if useLibcxx then libcxxStdenv else clangStdenv).mkDerivation rec {
      pname = "argparse";
      version = "3.2";

      src = fetchFromGitHub {
        owner = "p-ranav";
        repo = "argparse";
        rev = "v${version}";
        hash = "sha512-TGSBdWib2WSlIZp8hqYVC4YzcZDaGICPLlUfxg1XFIUAyV4v1L3gKxKtiy1w0aTCdRxFZwt++mnEdv4AX+zjdw==";
      };

      nativeBuildInputs = [
        cmake
        ninja
        pkg-config
      ];
      buildInputs = extraBuildInputs;

      cmakeFlags = [
        "-G"
        "Ninja"
        "-DARGPARSE_BUILD_TESTS=OFF"
        "-DARGPARSE_INSTALL_CMAKE_DIR=lib/cmake/argparse"
        "-DARGPARSE_INSTALL_PKGCONFIG_DIR=lib/pkgconfig"
        "-DCMAKE_INSTALL_INCLUDEDIR=include"
      ] ++ libcxxFlags;

      enableParallelBuilding = true;
      strictDeps = true;

      meta = with lib; {
        description = "Header-only C++17 argument parser";
        homepage = "https://github.com/p-ranav/argparse";
        license = licenses.mit;
        platforms = platforms.linux;
      };
    };

  parseWithSanitizerArgs = arg:
    if builtins.isList arg then {
      extraBuildInputs = arg;
      useLibcxx = false;
    } else if builtins.isAttrs arg then {
      extraBuildInputs =
        if arg ? extraBuildInputs then arg.extraBuildInputs
        else if arg ? extraPackages then arg.extraPackages
        else [ ];
      useLibcxx = arg.useLibcxx or false;
    } else {
      extraBuildInputs = [ ];
      useLibcxx = false;
    };

in {
  default = build { };
  withSanitizer = arg:
    let cfg = parseWithSanitizerArgs arg;
    in build {
      inherit (cfg) extraBuildInputs useLibcxx;
    };
}
