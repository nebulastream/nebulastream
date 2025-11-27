{
  lib,
  llvmPackages_19,
  cmake,
  ninja,
  pkg-config,
  fetchFromGitHub,
}:

let
  llvmPackages = llvmPackages_19;
  clangStdenv = llvmPackages.stdenv;
  libcxxStdenv = llvmPackages.libcxxStdenv;

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
    }:
    let
      libcxxFlags = lib.optionals useLibcxx [
        "-DCMAKE_CXX_FLAGS=-stdlib=libc++"
        "-DCMAKE_EXE_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_SHARED_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_MODULE_LINKER_FLAGS=-stdlib=libc++"
      ];
    in
    (if useLibcxx then libcxxStdenv else clangStdenv).mkDerivation rec {
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

      cmakeFlags = [
        "-G"
        "Ninja"
        "-DBUILD_EXAMPLES=OFF"
        "-DBUILD_TESTS=OFF"
        "-DBUILD_STRESS_TESTS=OFF"
        "-DBUILD_UNIT_TESTS=OFF"
        "-DBUILD_UNIVERSAL_BENCHMARK=OFF"
      ]
      ++ libcxxFlags;

      enableParallelBuilding = true;
      strictDeps = true;

      meta = with lib; {
        description = "C++ library providing fast concurrent hash tables";
        homepage = "https://github.com/efficient/libcuckoo";
        license = licenses.bsd3;
        platforms = platforms.linux;
      };
    };

  parseWithSanitizerArgs =
    arg:
    if builtins.isList arg then
      {
        extraBuildInputs = arg;
        useLibcxx = false;
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
      }
    else
      {
        extraBuildInputs = [ ];
        useLibcxx = false;
      };

in
{
  default = build { };
  withSanitizer =
    arg:
    let
      cfg = parseWithSanitizerArgs arg;
    in
    build {
      inherit (cfg) extraBuildInputs useLibcxx;
    };
}
