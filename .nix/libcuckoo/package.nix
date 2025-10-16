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
      pname = "libcuckoo";
      version = "0.3.1";

      src = fetchFromGitHub {
        owner = "efficient";
        repo = "libcuckoo";
        rev = "ea8c36c65bf9cf83aaf6b0db971248c6ae3686cf";
        hash = "sha512-kXfaPFyDC2x4osXD81zglOXIQG6CHxts2thp1V9FxGMdR+7kLGBJPtBoIMzPNR2biu3/xSpVlkXkM8Megh1gxA==";
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
        "-DBUILD_EXAMPLES=OFF"
        "-DBUILD_TESTS=OFF"
        "-DBUILD_STRESS_TESTS=OFF"
        "-DBUILD_UNIT_TESTS=OFF"
        "-DBUILD_UNIVERSAL_BENCHMARK=OFF"
      ];

      enableParallelBuilding = true;
      strictDeps = true;

      meta = with lib; {
        description = "C++ library providing fast concurrent hash tables";
        homepage = "https://github.com/efficient/libcuckoo";
        license = licenses.bsd3;
        platforms = platforms.linux;
      };
    };

in mkSanitizedPackage build
