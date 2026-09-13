# Running a fork of syncd as a background service

These are **templates**, not a ready-to-use installer: this repo is a
foundation meant to be forked (see the main [README](../README.md)'s
"Using this as a foundation"), and every real fork so far has ended up
with a slightly different binary name, service label, and default flags.
Copy this directory into your fork and fill in the few variables marked
at the top of each script — they're written to make that obvious.

```
service/
  linux/install.sh + syncd.service.template     systemd --user
  macos/install.sh + syncd.plist.template        launchd (LaunchAgent)
  windows/install.ps1                            Scheduled Task
```

All three do the same three things: build the daemon in release mode,
generate the service definition from the template (substituting your
binary's path and name), and register + start it — per-user, no
administrator/root rights needed anywhere, restarting automatically on
failure or at next login/boot.

## What to edit

Each script has a small block near the top:

```bash
BIN_NAME="syncd"     # the [[bin]] name in your fork's Cargo.toml
SERVE_ARG=""         # "serve" if your fork dispatches on a subcommand
                      # (see the main README's "two run modes" pattern),
                      # empty if your daemon takes flags directly like
                      # this repo's own syncd does
```

macOS also asks for a `LABEL` (reverse-DNS style, e.g.
`com.yourname.yourapp-syncd`) and Windows a `$TaskName` — both just need
to be unique on the machine, in case more than one fork's daemon (or more
than one of yours) ends up installed side by side.

## Passing daemon flags

Linux and macOS forward any extra arguments after `--` (or, on macOS,
baked into the plist — edit `syncd.plist.template`'s `ProgramArguments`
array directly for anything beyond what `SERVE_ARG` covers). Windows takes
`-ExtraArgs "..."`:

```bash
./linux/install.sh -- --device my-laptop --bootstrap 100.64.0.2:47100
```
```powershell
.\windows\install.ps1 -ExtraArgs "--device my-laptop --bootstrap 100.64.0.2:47100"
```

Most deployments need neither: `--device` auto-generates and persists a
unique id on first run (see `main.rs`'s `resolve_device_id`), and
discovery (tailnet, LAN broadcast, peer exchange) needs no address
configured at all.

## Uninstalling

There's no generic `uninstall.sh` here on purpose — it's three lines you
already have the tools for:

```bash
# Linux
systemctl --user disable --now $BIN_NAME
rm ~/.config/systemd/user/$BIN_NAME.service

# macOS
launchctl unload ~/Library/LaunchAgents/$LABEL.plist
rm ~/Library/LaunchAgents/$LABEL.plist
```
```powershell
# Windows
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
```
