{ pkgs }:
let
  lib = pkgs.lib;
  antlr4Version = "4.13.2";

  build = { extraBuildInputs ? [] }:
    pkgs.llvmPackages_19.stdenv.mkDerivation rec {
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
      ] ++ extraBuildInputs;

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
      ];

      enableParallelBuilding = true;
      strictDeps = true;

      meta = with lib; {
        description = "ANTLR4 C++ runtime for NebulaStream";
        homepage = "https://www.antlr.org";
        license = licenses.bsd3;
        platforms = platforms.linux;
      };
    };

in {
  default = build { };
  withSanitizer = extraPackages: build { extraBuildInputs = extraPackages; };
}
