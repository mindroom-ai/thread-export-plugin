# Thread Export

> [!IMPORTANT]
> **This plugin is archived.** Thread exports are now built into [MindRoom](https://github.com/mindroom-ai/mindroom): set `thread_exports: true` on any agent in `config.yaml`, or pass `invited_rooms` / `private_room_scope` as a mapping (see the [agent configuration docs](https://docs.mindroom.chat/configuration/agents/#thread-exports)). The upstreaming change is [mindroom-ai/mindroom#1954](https://github.com/mindroom-ai/mindroom/pull/1954). To migrate, remove this plugin from `plugins:` and enable `thread_exports` on the agents that were listed under the plugin's `agents` setting; the two per-agent options keep their names, and the `debounce_seconds` setting is gone (fixed at two seconds). This repository is kept read-only for reference.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-plugins-blue)](https://docs.mindroom.chat/plugins/)
[![Hooks](https://img.shields.io/badge/docs-hooks-blue)](https://docs.mindroom.chat/hooks/)

<img src="https://media.githubusercontent.com/media/mindroom-ai/mindroom/refs/heads/main/frontend/public/logo.png" alt="MindRoom Logo" align="right" width="120" />

Automatic Matrix thread exports for [MindRoom](https://github.com/mindroom-ai/mindroom) agents.

When enabled for an agent, threads from every Matrix room that agent is currently joined to are continuously exported as YAML files into its workspace at predictable paths, so the agent's file and shell tools can grep its conversation history without any Matrix API access.

## Features

- Exports threads from every room the enabled agent is currently joined to into `<workspace>/thread_exports/<room>/<thread>.yaml` (the same layout as `mindroom threads export`)
- Optionally covers user-created rooms too, while still requiring the enabled agent to be currently joined
- Re-exports a room shortly after every message in it, plus one full pass at startup and after config hot reload
- Projection-first: thread bodies are served from MindRoom's durable event-journal visible-message projection, hydrating unread threads from the homeserver only when needed
- Skip-unchanged writes: files are only rewritten when thread content actually changed
- Reconciles removed threads and revoked room access so stale exports do not survive
- Fetches each source thread once and fans it out to every authorized workspace
- Debounced single-flight runner: bursts of messages coalesce into one export pass

## How It Works

1. `bot:ready` (router) queues one full export pass at startup.
2. `config:reloaded` queues a full pass after hot reload, including cleanup for agents removed from the plugin settings.
3. `message:received` and `message:after_response` enqueue the affected room and requester without waiting for filesystem access.
4. A background runner debounces triggers, discovers private roots, then reads each dirty room once and fans the result out only to enabled agents that are currently joined.
5. When discovery changes the private-root index, the runner queues a full reconciliation for its next pass.
6. Unchanged thread files are left untouched, while vanished threads and unauthorized room directories are removed.

## Hooks

| Hook | Event | Purpose |
|------|-------|---------|
| `thread-export-startup` | `bot:ready` | Queue one full export pass once the router is ready |
| `thread-export-config-reloaded` | `config:reloaded` | Queue one full export pass after config hot reload |
| `thread-export-on-message` | `message:received` | Queue the message's room, or a full pass for a newly discovered private root |
| `thread-export-after-response` | `message:after_response` | Queue the responded room, or a full pass for a newly discovered private root |

## Settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `agents` | (none) | Agents whose workspaces receive exports: a list of names, or a mapping of name to per-agent options. Missing or empty disables the plugin |
| `agents.<name>.invited_rooms` | `true` | Whether this agent's exports also consider rooms joined through invites (user-created rooms); current membership is always required |
| `agents.<name>.private_room_scope` | `owner_and_agent` | Private agents only: require either owner membership, or both owner and managed-agent membership |
| `debounce_seconds` | `2` | Delay after the last trigger before an export pass runs |

Per-agent options example:

```yaml
plugins:
  - path: plugins/thread-export
    settings:
      agents:
        code:
          invited_rooms: false   # config rooms only
        research:
          private_room_scope: owner  # all rooms visible to each private owner
```

## Output Layout

```
<storage_root>/agents/<agent>/workspace/thread_exports/<urlencoded room key>/<urlencoded thread id>.yaml
```

Inside the agent's own tools this is `$MINDROOM_AGENT_WORKSPACE/thread_exports/` (the workspace is the agent's `$HOME`).
Each file is the standard thread export document: `version`, `room` metadata, `thread` metadata (including the latest MindRoom thread summary as `thread.summary`), and a `messages` list.
Each room directory also contains an `index.json` mapping every thread file to its message count, participants, latest summary, and last activity, sorted by most recent activity, so agents can navigate a room without opening every thread file.

Private agents (`private:` config) are supported: every existing private instance gets its own copy under its requester-scoped workspace:

```
<storage_root>/private_instances/<worker scope>/<agent>/<private root>/thread_exports/...
```

Instances are discovered from their core identity records, so a brand-new requester's instance starts receiving exports after its runtime has been materialized.

Shared-agent exports are scoped to the enabled agent's current room memberships.
Private-instance exports are scoped to the owner's current memberships, so one requester's private workspace never accumulates other users' conversations.
Each private instance is authorized only when its core identity record names a valid Matrix requester and forward-resolves to that instance's state root. The plugin retains only validated roots observed by message and response hooks; startup and configuration reload rebuild that bounded in-memory index from the core records. Missing, unreadable, malformed, or mismatched records remove prior exports and prevent new ones.
Membership lookup failures block new exports for that room and are reported, while previously authorized files remain until a successful lookup definitively proves that access was revoked.

## Semantic Search Over Exports

Without any extra config, agents already have file-based search: the exports are plain YAML in the agent's workspace, and each room's `index.json` maps threads to participants and summaries, so `grep`/`read` file tools cover keyword search.

With an embedder configured (`memory.embedder`), point a knowledge base at the export directory to get semantic search through the standard `search_knowledge_base` tool.

Shared agent (paths resolve against the config.yaml directory; default storage is `./mindroom_data`):

```yaml
knowledge_bases:
  code_threads:
    path: ./mindroom_data/agents/code/workspace/thread_exports
    description: Exported Matrix conversation history for the code agent
agents:
  code:
    knowledge_bases: [code_threads]
```

Private agent (each instance indexes its own owner-scoped exports; `path` is relative to the private root):

```yaml
agents:
  secret:
    private:
      per: user
      knowledge:
        path: thread_exports
        description: Your exported conversation history
```

Notes:

- `mode: semantic` is the knowledge-base default; `.yaml` and `.json` are in the default indexed extension set, so no extension config is needed.
- The active thread's file rewrites on every message, so a watching semantic index re-embeds that thread per message. This is negligible with a local embedder (Ollama, sentence-transformers) but costs real money with paid embedding APIs in busy rooms.
- Add `exclude_patterns: ["*/index.json"]` to the knowledge base if you prefer to keep the room indexes out of the semantic index.

## Install

Requires MindRoom v2026.8.136 or newer.

Vendor this plugin with the MindRoom CLI:

```bash
mindroom plugins install thread-export-plugin
```

Then reference it from `config.yaml`:

```yaml
plugins:
  - path: plugins/thread-export-plugin
```

Update to the latest commit later with:

```bash
mindroom plugins update thread-export-plugin
```

The command pins the exact installed commit in `.mindroom-plugin.lock.json` and strictly validates the plugin before activating it.
For a manual checkout instead, see Setup below.

## Setup

1. Copy this plugin to `~/.mindroom/plugins/thread-export` (or reference it by relative path).
2. Add the plugin to `config.yaml` (relative paths resolve against the config file's directory):

   ```yaml
   plugins:
     - path: plugins/thread-export
       settings:
         agents: [code, research]
         debounce_seconds: 2
   ```

3. Restart MindRoom (or let config hot reload pick it up).

## Notes

- Cost profile: thread bodies normally come from the local event-journal projection; threads not yet projected may be hydrated once from the homeserver, regardless of how many workspace targets receive them.
- Every shared and private export target is membership-scoped. The `invited_rooms` option only controls whether user-created invited rooms are considered; it never bypasses the membership check.
- Private targets default to `owner_and_agent`, so inviting the managed agent is the explicit opt-in for exporting a room. Set `private_room_scope: owner` to export every room where the private instance's owner is currently joined.
- Agents may edit or delete their exported YAML files; deleted files are restored on the next pass that touches the room and on the next startup pass.
