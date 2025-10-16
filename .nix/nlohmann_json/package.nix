{ pkgs }:
let
  lib = pkgs.lib;
  sanitizerSupport = import ../lib/sanitizer-support.nix { inherit lib; };
  mkSanitizedPackage = sanitizerSupport.mkSanitizedPackage;

  mergeFlags = old: extra:
    let
      existing =
        if builtins.isNull old then "" else old;
      combined =
        lib.concatStringsSep " "
          (lib.filter (flag: flag != "") [ existing extra ]);
    in
    if combined == "" then null else combined;

  build = {
    extraBuildInputs ? [ ],
    sanitizeCompileFlags ? [ ],
    sanitizeLinkFlags ? [ ]
  }:
    pkgs.nlohmann_json.overrideAttrs (old: let
      sanitizedCompileFlags = lib.concatStringsSep " " sanitizeCompileFlags;
      sanitizedLinkFlags = lib.concatStringsSep " " sanitizeLinkFlags;
      packagePatch = ./patches/0001-fix-missing-cstdio-header-for-missing-EOF.patch;
      flagCandidates = {
        NIX_CFLAGS_COMPILE = mergeFlags (old.NIX_CFLAGS_COMPILE or null) sanitizedCompileFlags;
        NIX_CXXFLAGS_COMPILE = mergeFlags (old.NIX_CXXFLAGS_COMPILE or null) sanitizedCompileFlags;
        NIX_CFLAGS_LINK = mergeFlags (old.NIX_CFLAGS_LINK or null) sanitizedLinkFlags;
      };
      flagUpdates = lib.filterAttrs (_: value: ! builtins.isNull value) flagCandidates;
      flagNames = [ "NIX_CFLAGS_COMPILE" "NIX_CXXFLAGS_COMPILE" "NIX_CFLAGS_LINK" ];
      envWithoutFlags =
        lib.filterAttrs (name: _: !(lib.elem name flagNames)) (old.env or { });
    in
      let
        version =
          if builtins.hasAttr "version" old && ! builtins.isNull old.version
          then old.version
          else "3.12.0";
      in
      {
        src = pkgs.fetchFromGitHub {
          owner = "nlohmann";
          repo = "json";
          rev = "v${version}";
          hash = "sha256-cECvDOLxgX7Q9R3IE86Hj9JJUxraDQvhoyPDF03B2CY=";
        };

        patches =
          lib.unique (
            (old.patches or [ ])
            ++ lib.optional (lib.elem packagePatch (old.patches or [ ]) == false) packagePatch
          );
        buildInputs = (old.buildInputs or [ ]) ++ extraBuildInputs;

        cmakeFlags = lib.unique ((old.cmakeFlags or [ ]) ++ [
          "-DCMAKE_INSTALL_INCLUDEDIR=include"
          "-DJSON_BuildTests=OFF"
          "-DJSON_FastTests=OFF"
          "-DJSON_MultipleHeaders=ON"
        ]);
      }
      // flagUpdates
      // { env = envWithoutFlags; });

in mkSanitizedPackage build
