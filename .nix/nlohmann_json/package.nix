{ pkgs }:
let
  lib = pkgs.lib;
  llvm = pkgs.llvmPackages_19;
  clangStdenv = llvm.stdenv;

  build = { extraBuildInputs ? [ ] }:
    clangStdenv.mkDerivation (finalAttrs: {
      pname = "nlohmann_json";
      version = "3.12.0";

      src = pkgs.fetchFromGitHub {
        owner = "nlohmann";
        repo = "json";
        rev = "v${finalAttrs.version}";
        hash = "sha256-cECvDOLxgX7Q9R3IE86Hj9JJUxraDQvhoyPDF03B2CY=";
      };

      patches = [ ./patches/0001-fix-missing-cstdio-header-for-missing-EOF.patch ];

      nativeBuildInputs = [ pkgs.cmake ];
      buildInputs = extraBuildInputs;

      cmakeFlags = [
        "-DCMAKE_INSTALL_INCLUDEDIR=include"
        "-DJSON_BuildTests=OFF"
        "-DJSON_FastTests=OFF"
        "-DJSON_MultipleHeaders=ON"
      ];

      strictDeps = true;

      meta = with lib; {
        description = "JSON for Modern C++";
        homepage = "https://json.nlohmann.me";
        changelog = "https://github.com/nlohmann/json/blob/develop/ChangeLog.md";
        license = licenses.mit;
        platforms = platforms.all;
      };
    });
in {
  default = build { };
  withSanitizer = extraPackages: build { extraBuildInputs = extraPackages; };
}
