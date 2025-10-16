{ lib
, llvmPackages_19
, cmake
, ninja
, pkg-config
, fetchFromGitHub
}:

let
  sanitizerSupport = import ../lib/sanitizer-support.nix { inherit lib; };
  mkSanitizedPackage = sanitizerSupport.mkSanitizedPackage;
  clangStdenv = llvmPackages_19.stdenv;

  build = {
    extraBuildInputs ? [],
    sanitizeCompileFlags ? [],
    sanitizeLinkFlags ? []
  }:
    clangStdenv.mkDerivation rec {
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

      sanitizedCompileFlags = lib.concatStringsSep " " sanitizeCompileFlags;
      sanitizedLinkFlags = lib.concatStringsSep " " sanitizeLinkFlags;

      NIX_CFLAGS_COMPILE = lib.optionalString (sanitizedCompileFlags != "") sanitizedCompileFlags;
      NIX_CXXFLAGS_COMPILE = lib.optionalString (sanitizedCompileFlags != "") sanitizedCompileFlags;
      NIX_CFLAGS_LINK = lib.optionalString (sanitizedLinkFlags != "") sanitizedLinkFlags;

      cmakeFlags = [
        "-G"
        "Ninja"
        "-DARGPARSE_BUILD_TESTS=OFF"
        "-DARGPARSE_INSTALL_CMAKE_DIR=lib/cmake/argparse"
        "-DARGPARSE_INSTALL_PKGCONFIG_DIR=lib/pkgconfig"
        "-DCMAKE_INSTALL_INCLUDEDIR=include"
      ];

      enableParallelBuilding = true;
      strictDeps = true;

      meta = with lib; {
        description = "Header-only C++17 argument parser";
        homepage = "https://github.com/p-ranav/argparse";
        license = licenses.mit;
        platforms = platforms.linux;
      };
    };

in mkSanitizedPackage build
