# dapla-shared-data-processor-build-scripts
Actions and scripts used to build, deploy and test images used by Delomaten's data sharing feature.

## Development

This project uses the [mill build tool](https://mill-build.org/mill/index.html). For instructions on how to integrate this
with your IDE see [these docs](https://mill-build.org/mill/cli/installation-ide.html#_ide_support). To run mill from the CLI simply uses the provided `./mill` script in the repository
or the provided nix development environment through `nix develop`. In either case there is no need to install `mill` yourself.

### Setting up coursier credentials

Since we rely on packages published to 'github packages' it's necessary to provide credentials to coursier for local development
so that the packages can be read.

Grant GitHub CLI permission to read packages:

```sh
gh auth refresh -s read:packages
```

To authenticate Coursier in the current shell session, set the environment variable:

```sh
export COURSIER_CREDENTIALS="maven.pkg.github.com $(gh api user --jq .login):$(gh auth token)"
```

For a persistent setup, add your GitHub credentials to the Coursier [property file](https://get-coursier.io/docs/other-credentials#property-file). Your GitHub PAT token can be found using the `gh auth token` command.

```
simple.username=<GITHUB USERNAME>
simple.password=<GITHUB PAT TOKEN>
simple.host=maven.pkg.github.com
```
