# CLion plugins

Container creation installs NES-Systest-Runner 1.5 and
[IDE Index MCP Server](https://github.com/hechtcarmel/jetbrains-index-mcp-plugin)
5.20.3 into the CLion 2026.2 backend. Both release downloads are checksum-pinned.

The index plugin installer also applies `mcp-plugin.xml`, selecting the same 35
tools enabled in the CLion backend, including `ide_open_file` and
`ide_get_active_file`. It replaces the exposed-tool selection on every run while
preserving other MCP preferences, such as the server port and history size.

Set `NES_CLION_VERSION` in `containerEnv` for another CLion version, or set
`NES_CLION_PLUGINS_DIR` to override the backend plugins directory directly.
Settings default to `${XDG_CONFIG_HOME:-$HOME/.config}/JetBrains/CLion2026.2`
(using `NES_CLION_VERSION` when set). Set `NES_CLION_CONFIG_DIR` to override that
configuration directory.

For an existing container, stop the CLion backend, run the installer, then start
the backend again so it loads the settings without overwriting them on shutdown:

```sh
bash .devcontainer/install-index-mcp-plugin.sh
```

After opening a project, the MCP plugin's default CLion endpoint is
`http://127.0.0.1:29177/index-mcp/streamable-http` inside the container. Configure
agents using the plugin's **Install on Coding Agents** button or its upstream
documentation.

## Pi extensions

Pi extensions are maintained separately in
[Artraxon/pi-setup](https://github.com/Artraxon/pi-setup), checked out locally at
`.devcontainer/pi/`. NebulaStream ignores that entire directory, including its
nested Git repository; it is also excluded from the Docker build context.

On container creation, `setup-pi.sh` clones the repository over HTTPS if missing,
or pulls `main` with `--ff-only` if already present, then runs its installer.
It refuses to update dirty checkouts, unexpected origins/branches, or divergent
history rather than overwriting local work. To update an existing container:

```sh
bash .devcontainer/setup-pi.sh
```

Override `PI_SETUP_REPOSITORY` and/or `PI_SETUP_BRANCH` to use a fork or another
branch. To reinstall the current checkout without pulling, run
`bash .devcontainer/pi/install.sh`.

See [`pi/README.md`](pi/README.md) for shortcuts, PTY behavior, requirements, and
tests. Save active PTY work before `/reload`, since reload terminates those
sessions. The native shell-color runtime patch requires restarting Pi.

### Pinned OpenAI Toolkit

After installing the custom extensions, `setup-pi.sh` installs
[pi-openai-toolkit](https://github.com/awoaCrim/pi-openai-toolkit) at reviewed commit
`37787051aa6e566dad0c028d49e20ded3ec72f25` using Pi's Git-package manager.
The full commit is pinned in `install-pi-openai-toolkit.sh`; it is not a moving
branch or tag. The package declaration and checkout persist in `/root/.pi`.
To install/reconcile only this package:

```sh
bash .devcontainer/install-pi-openai-toolkit.sh
```

No Toolkit policy is overridden: its default remote compaction applies to
eligible models, image generation stays off, and web search remains unmanaged.
Request/response capture stays off by default; keep it disabled unless debugging.
The installer also applies `patches/pi-openai-toolkit-hide-fast-status.patch`:
only the persistent `Fast ON/OFF/N/A` status indicator is hidden. `/fast`, its
notifications, session preference, and request behavior remain unchanged. The
patch is idempotent and fails rather than guessing if the pinned source changes.
Reapply it with the same installer after manually reconciling package updates.
Run `/reload` after installation (save active PTY work first).

The devcontainer retains the integration settings: installing the Pi CLI in the
Docker image, persisting `/root/.pi`, setting `EDITOR=vim`, and calling the
extension installer. CLion backend plugin installers remain here because they
are container/IDE setup rather than Pi extensions.
