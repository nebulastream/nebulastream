{ pkgs }:
let
  lib = pkgs.lib;
  antlr4Version = "4.13.2";
  llvmPackages = pkgs.llvmPackages_19;

  stdenvFor = useLibcxx: if useLibcxx then llvmPackages.libcxxStdenv else llvmPackages.stdenv;

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
    (stdenvFor useLibcxx).mkDerivation rec {
      pname = "antlr4";
      version = antlr4Version;

      src = pkgs.fetchFromGitHub {
        owner = "antlr";
        repo = "antlr4";
        rev = antlr4Version;
        hash = "sha512-dSzBW7T1Qo8S1StnIsRfW3dcXNdGYI19KZPtVX2sY6Uyd5j4mwfwXu5n/ncDaUd3Fs8KcjcLUNrXWxCZTEdTdg==";
      };

      sourceRoot = "source/runtime/Cpp";

      nativeBuildInputs = [
        pkgs.cmake
        pkgs.ninja
        pkgs.pkg-config
      ];
      buildInputs = [
        pkgs.libuuid
        pkgs.zlib
      ]
      ++ extraBuildInputs;

      postPatch = ''
        substituteInPlace runtime/CMakeLists.txt \
          --replace "target_compile_definitions(antlr4_shared PUBLIC ANTLR4CPP_EXPORTS)" "target_compile_definitions(antlr4_shared PRIVATE ANTLR4CPP_EXPORTS)"
        substituteInPlace runtime/src/atn/ProfilingATNSimulator.cpp \
          --replace "#include \"atn/ProfilingATNSimulator.h\"" "#include \"atn/ProfilingATNSimulator.h\"\n#include <chrono>"
      '';

      cmakeFlags = [
        "-G"
        "Ninja"
        "-DANTLR_BUILD_STATIC=ON"
        "-DANTLR_BUILD_SHARED=ON"
        "-DANTLR4_INSTALL=ON"
        "-DANTLR_BUILD_CPP_TESTS=OFF"
      ]
      ++ libcxxFlags;

      enableParallelBuilding = true;
      strictDeps = true;

      meta = with lib; {
        description = "ANTLR4 C++ runtime for NebulaStream";
        homepage = "https://www.antlr.org";
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
