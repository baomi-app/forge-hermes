# Forge Hermes Plugin

This directory is a Hermes platform plugin. It lets a Hermes gateway pair with
Forge Console the same way messaging platforms do: Hermes initiates the
connection from the runtime side.

## Install

From this repository:

```bash
mkdir -p ~/.hermes/plugins
cp -R forge-hermes ~/.hermes/plugins/forge
```

Then create a pairing code in Forge Console:

1. Open `Agents`.
2. Click `New`.
3. Choose `Hermes`.
4. Click `Create pairing code`.

Configure Hermes with the values shown by Forge:

```bash
hermes config set FORGE_SERVER_URL https://api.forge.baomi.app
hermes config set FORGE_PAIRING_CODE ABCD-EFGH
hermes config set FORGE_RUNTIME_NAME Hermes
hermes gateway
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

Forge can also ask the plugin to query the local Hermes API for runtime-owned
state such as sessions, scheduled jobs, job runs, and run events. Configure how
the plugin reaches that API on the machine running this Hermes runtime:

```bash
hermes config set FORGE_HERMES_API_URL https://hermes.baomi.app
hermes config set FORGE_HERMES_API_KEY your-hermes-api-server-key
```

Forge does not infer these values during pairing. Pairing only creates the
Forge channel; API inspection uses the Hermes API settings local to the paired
runtime.

The same values can be provided as environment variables. The plugin checks
`FORGE_HERMES_API_URL`, `HERMES_API_URL`, `API_SERVER_URL`, then
`HERMES_ENDPOINT` for the API URL, and `FORGE_HERMES_API_KEY`,
`HERMES_API_KEY`, `API_SERVER_KEY`, then `HERMES_API_TOKEN` for the bearer key.

If you want the adapter to reconnect without a new pairing code, persist the
returned channel values as:

```bash
hermes config set FORGE_CHANNEL_URL https://api.forge.baomi.app/api/runtime-channels/agent_xxx
hermes config set FORGE_CHANNEL_TOKEN your-channel-token
```

## Current Status

This is an alpha adapter. Message replies use the Forge runtime channel.
Sessions, automations, runs, and run events are command-proxied back to the
Hermes API so Hermes remains the source of truth.

Keeping this as a Hermes plugin avoids patching Hermes core and keeps the
runtime connection usable from private networks.
