{
  description = "Dev env for shared data processor";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = inputs @ {flake-parts, ...}:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = ["x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin"];
      perSystem = {
        self',
        pkgs,
        ...
      }: {
        formatter = pkgs.alejandra;
        devShells.default = pkgs.mkShell {
          name = "dev shell";
          packages = with pkgs; [
            openjdk
            nixd
            python312
            python312Packages.flake8
            ruff
            uv
            yaml-language-server
            scala-cli
            metals
            self'.packages.mill
          ];
        };
        packages.mill = pkgs.callPackage ./nix/mill/package.nix {};
      };
    };
}
