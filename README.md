# Thread Export

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-plugins-blue)](https://docs.mindroom.chat/plugins/)
[![Hooks](https://img.shields.io/badge/docs-hooks-blue)](https://docs.mindroom.chat/hooks/)

<img src="https://media.githubusercontent.com/media/mindroom-ai/mindroom/refs/heads/main/frontend/public/logo.png" alt="MindRoom Logo" align="right" width="120" />

Automatic Matrix thread exports for [MindRoom](https://github.com/mindroom-ai/mindroom) agents.

When enabled for an agent, every Matrix thread is continuously exported as a YAML file into that agent's workspace at a predictable path, so the agent's file and shell tools can grep full conversation history without any Matrix API access.

## Features

- Exports all rooms' threads into `<workspace>/thread_exports/<room>/<thread>.yaml` (the same layout as `mindroom threads export`)
- Covers user-created rooms too: rooms agents joined via invite are exported with the invited agent's own account
- Re-exports a room shortly after every message in it, plus one full pass at startup
- Cache-first: thread bodies are served from MindRoom's durable event cache, so passes barely touch the homeserver
- Skip-unchanged writes: files are only rewritten when thread content actually changed
- Debounced single-flight runner: bursts of messages coalesce into one export pass

## How It Works

1. `bot:ready` (router) queues one full export pass at startup.
2. `message:received` and `message:after_response` mark the affected room dirty.
3. A background runner debounces triggers, then runs `export_threads_once(prefer_cache=True)` for each dirty room into every enabled agent's workspace.
4. Unchanged thread files are left untouched, so `exported_at` reflects the last content change.

## Hooks

| Hook | Event | Purpose |
|------|-------|---------|
| `thread-export-startup` | `bot:ready` | Queue one full export pass once the router is ready |
| `thread-export-on-message` | `message:received` | Mark the message's room dirty |
| `thread-export-after-response` | `message:after_response` | Mark the responded room dirty |

## Settings

| Setting | Default | Purpose |
|---------|---------|---------|
| `agents` | (none) | Agent names whose workspaces receive exports. Missing or empty disables the plugin |
| `debounce_seconds` | `2` | Delay after the last trigger before an export pass runs |

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

Instances are discovered on disk, so a brand-new requester's instance starts receiving exports from the first pass after the instance is created.

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

- Cost profile: each pass performs one thread-list call per dirty room against the homeserver; thread bodies come from the local event cache.
- Every enabled agent receives a full copy of all rooms' threads; do not enable it for agents that should not see other rooms' conversations.
- Agents may edit or delete their exported YAML files; deleted files are restored on the next pass that touches the room and on the next startup pass.
