# Forge Hermes Plugin

This directory is a Hermes platform plugin. It lets a Hermes gateway pair with
Forge Console the same way messaging platforms do: Hermes initiates the
connection from the runtime side.

## Install

Install the plugin on the machine that runs `hermes gateway`.

From a checkout of the Forge monorepo:

```bash
mkdir -p ~/.hermes/plugins
cp -R forge-hermes ~/.hermes/plugins/forge
```

Or install directly from GitHub:

```bash
mkdir -p ~/.hermes/plugins
rm -rf ~/.hermes/plugins/forge
git clone https://github.com/baomi-app/forge-hermes.git ~/.hermes/plugins/forge
```

Restart the Hermes gateway so it loads the plugin:

```bash
hermes gateway restart
```

If you run the gateway in the foreground, stop it and start it again:

```bash
hermes gateway run
```

## Pair With Forge Console

Create a pairing code in Forge Console:

1. Open `Agents`.
2. Click `New`.
3. Choose `Hermes`.
4. Click `Create pairing code`.

On the Hermes machine, run the command shown by Forge Console. The server URL
is deployment-specific and should come from your Forge Console, not from this
README:

```bash
hermes config set FORGE_SERVER_URL https://your-forge-server.example.com
hermes config set FORGE_PAIRING_CODE ABCD-EFGH
hermes config set FORGE_RUNTIME_NAME Hermes
hermes gateway restart
```

The plugin calls:

```text
POST /api/agent-registrations/connect
```

and Forge creates the agent record for the workspace that generated the pairing
code.

After pairing, Forge returns a private channel URL and bearer token. The adapter
uses that channel to:

- poll `GET /api/runtime-channels/:agentId/runtime/poll` for user messages;
- call Hermes with `handle_message`;
- post replies to `POST /api/runtime-channels/:agentId/runtime/messages`.

## Runtime Inspection

Messages only need the Forge channel above. Forge Console can also ask the
plugin to inspect runtime-owned state such as sessions, scheduled jobs, job
runs, and run events.

For inspection, enable the Hermes API server on the same machine. The plugin
auto-discovers the local Hermes API settings from `~/.hermes/.env`,
`~/.hermes/config.yaml`, or the process environment:

```bash
hermes config set API_SERVER_ENABLED true
hermes config set API_SERVER_KEY your-hermes-api-server-key
hermes gateway restart
```

No Forge-specific Hermes API URL/key is required. The plugin defaults to the
local Hermes API server at `http://127.0.0.1:8642` and reads `API_SERVER_KEY`
locally. Legacy overrides are still supported for debugging:
`FORGE_HERMES_API_URL`, `HERMES_API_URL`, `API_SERVER_URL`, or
`HERMES_ENDPOINT` for the API URL, and `FORGE_HERMES_API_KEY`,
`HERMES_API_KEY`, `API_SERVER_KEY`, or `HERMES_API_TOKEN` for the bearer key.

If runtime inspection is not configured, Forge channel messaging still works,
but Console views that depend on Hermes-owned state may be unavailable.

If you want the adapter to reconnect without a new pairing code, persist the
returned channel values from the pairing response:

```bash
hermes config set FORGE_CHANNEL_URL https://your-forge-server.example.com/api/runtime-channels/agent_xxx
hermes config set FORGE_CHANNEL_TOKEN your-channel-token
```

## Current Status

This is an alpha adapter. Message replies use the Forge runtime channel.
Sessions, automations, runs, and run events are command-proxied to the local
Hermes API discovered on the Hermes machine, so Hermes remains the source of
truth without requiring Forge to know the Hermes API endpoint.

Keeping this as a Hermes plugin avoids patching Hermes core and keeps the
runtime connection usable from private networks.
