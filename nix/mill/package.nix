{
  autoPatchelfHook,
  fetchurl,
  lib,
  makeWrapper,
  openjdk,
  sourcesJSON ? ./sources.json,
  stdenvNoCC,
  zlib,
}:
stdenvNoCC.mkDerivation (finalAttrs: {
  pname = "mill";
  version = "1.0.3";
  src =
    let
      source = (lib.importJSON sourcesJSON)."${stdenvNoCC.system}";
    in fetchurl {
      url = "https://repo1.maven.org/maven2/com/lihaoyi/mill-dist-${source.artifact-suffix}/${finalAttrs.version}/mill-dist-${source.artifact-suffix}-${finalAttrs.version}.exe";
      inherit (source) hash;
    };

  dontUnpack = true;

  buildInputs = [openjdk zlib];
  nativeBuildInputs = [makeWrapper] ++ lib.optional stdenvNoCC.isLinux autoPatchelfHook;

  installPhase = ''
    runHook preInstall

    install -Dm 755 $src $out/bin/mill
    wrapProgram $out/bin/mill \
      --set-default JAVA_HOME "${openjdk}"

    runHook postInstall
  '';
})
