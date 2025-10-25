{ pkgs }:
let
  follyPatch = ./patches/0001-Folly-Patch.patch;
  follySrc = pkgs.fetchFromGitHub {
    owner = "facebook";
    repo = "folly";
    rev = "c47d0c778950043cbbc6af7fde616e9aeaf054ca";
    hash = "sha256-5f7IA/M5oJ/Sg5Z9RMB6PvaHzkXiqn1zZJ+QamZXONs=";
  };
  inherit (pkgs) lib;

  removePatchBySuffix = suffix: patches:
    builtins.filter (
      patch: !lib.hasSuffix suffix (builtins.baseNameOf (toString patch))
    ) patches;

in pkgs.folly.overrideAttrs (old: {
  version = "unstable-2024-09-04";
  src = follySrc;
  patches = removePatchBySuffix "folly-fix-glog-0.7.patch" (old.patches or [])
    ++ [ follyPatch ];
  postPatch = "";
})
