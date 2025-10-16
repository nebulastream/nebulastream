{ lib }:

let
  # Merge an existing space separated flag string with an additional string
  mergeFlags = old: extra:
    let
      existing =
        if builtins.isNull old then "" else old;
      combined =
        lib.concatStringsSep " "
          (lib.filter (flag: flag != "") [ existing extra ]);
    in
    if combined == "" then null else combined;

  sanitizePackage =
    {
      sanitizer,
      drv,
      extraBuildInputs ? [ ],
      disableChecks ? false
    }:
      let
        compileFlags = sanitizer.compileFlags or [ ];
        linkFlags = sanitizer.linkFlags or [ ];
        sanitizerInputs = sanitizer.extraPackages or [ ];
        sanitizedCompileFlags = lib.concatStringsSep " " compileFlags;
        sanitizedLinkFlags = lib.concatStringsSep " " linkFlags;
        combinedInputs = sanitizerInputs ++ extraBuildInputs;
        needsTweaks =
          sanitizedCompileFlags != ""
          || sanitizedLinkFlags != ""
          || combinedInputs != [ ];
      in
      if !needsTweaks && !disableChecks then drv else
        drv.overrideAttrs (old: let
          flagCandidates = {
            NIX_CFLAGS_COMPILE = mergeFlags (old.NIX_CFLAGS_COMPILE or null) sanitizedCompileFlags;
            NIX_CXXFLAGS_COMPILE = mergeFlags (old.NIX_CXXFLAGS_COMPILE or null) sanitizedCompileFlags;
            NIX_CFLAGS_LINK = mergeFlags (old.NIX_CFLAGS_LINK or null) sanitizedLinkFlags;
          };
          flagUpdates = lib.filterAttrs (_: value: ! builtins.isNull value) flagCandidates;
          flagNames = lib.attrNames flagCandidates;
          envWithoutFlags =
            lib.filterAttrs (name: _: !(lib.elem name flagNames)) (old.env or { });
          disableChecksAttr =
            lib.optionalAttrs (disableChecks && needsTweaks) { doCheck = false; };
        in
          {
            buildInputs = (old.buildInputs or [ ]) ++ combinedInputs;
          }
          // flagUpdates
          // (lib.optionalAttrs (flagUpdates != { }) { env = envWithoutFlags; })
          // disableChecksAttr);

  sanitizeArgs = sanitizer: {
    extraBuildInputs = sanitizer.extraPackages or [ ];
    sanitizeCompileFlags = sanitizer.compileFlags or [ ];
    sanitizeLinkFlags = sanitizer.linkFlags or [ ];
  };

  mkSanitizedPackage =
    buildFun:
      {
        default = buildFun { };
        withSanitizer = sanitizer:
          buildFun (sanitizeArgs sanitizer);
      };

in {
  inherit sanitizePackage mkSanitizedPackage sanitizeArgs;
}
