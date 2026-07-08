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
hermes config set FORGE_SERVER_URL https://forge-server.example.workers.dev
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

If you want the adapter to reconnect without a new pairing code, persist the
returned channel values as:

```bash
hermes config set FORGE_CHANNEL_URL https://forge-server.example.workers.dev/api/runtime-channels/agent_xxx
hermes config set FORGE_CHANNEL_TOKEN your-channel-token
```

## Current Status

This is an alpha adapter. Sessions and message replies are supported through
the Forge runtime channel. Automations still run through Hermes' native runtime
APIs until the Forge channel grows a job event protocol.

Keeping this as a Hermes plugin avoids patching Hermes core and keeps the
runtime connection usable from private networks.
