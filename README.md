# Claude Code Discord bridge

This builder-side Rust service connects Discord to Claude Code sessions that it starts locally. It uses your existing Claude Code login; it does not use an Anthropic API key. Each project gets a dedicated Discord text channel and its own tmux session.

## Discord setup

1. In the [Discord Developer Portal](https://discord.com/developers/applications), create an application, add a bot, and copy its token. Keep the token private.
2. On the Bot page, enable **Message Content Intent**.
3. Invite the bot using OAuth2 URL Generator with the `bot` scope and these permissions: View Channels, Send Messages, Read Message History, and **Manage Channels**. Manage Channels lets the bot create project channels.
4. In your server, optionally create a category for bridge channels. Set the category permissions to the people who should be able to read/use these Claude sessions. The bot-created channels inherit the category's access. Copy the category ID if using it.
5. Enable Developer Mode in Discord settings. Right-click your user and server to copy your user ID and server ID.

## Builder setup

Install Rust, tmux, and Claude Code on the builder. Make sure Claude Code is already authenticated for the OS account that will run the bridge. Project folders must be direct children of `PROJECTS_ROOT`.

Copy `.env.example` to `.env`, then set the bot token, server ID, allowed user ID(s), projects root, and a random hook secret. Set `DISCORD_CATEGORY_ID` if you want the channels inside a dedicated category. Multiple allowed Discord user IDs can be comma-separated. The secret can be generated with `openssl rand -hex 32`.

## Run it as a background service

The bridge is designed to stay running on the builder under systemd, so you do not need to keep an SSH terminal open. After configuring `.env`, install and start the user service:

```sh
./deploy/install-user-service.sh
```

Check status and follow logs with:

```sh
systemctl --user status claude-discord-bridge
journalctl --user -u claude-discord-bridge -f
```

To have the user service start at boot even before you SSH in, enable lingering for your builder account (this may require an administrator):

```sh
sudo loginctl enable-linger "$USER"
```

Then manage it with `systemctl --user restart claude-discord-bridge` or `systemctl --user stop claude-discord-bridge`. The installer builds the release binary and configures automatic restart after a crash. The service runs as your normal user so it can use that user's Claude Code login and project files.

## Use

DM the bot, or mention it in the configured server:

```text
@HomeLab start my-project
```

In a bot DM, send `start my-project` without the mention.

The bot creates (or reuses) `#claude-my-project` in the configured server, starts Claude Code in that project's directory, and replies with a channel link. A project is the folder directly under `PROJECTS_ROOT`, and its channel/session stays linked to that codebase. `!status` shows the full local path. Continue the conversation in that channel. Each message there is sent to that project's running Claude terminal, and Claude's completed text replies are posted back into the channel. Use `list` in the bot DM to see channels already registered by this process.

Use `!sessions` in a project channel to list existing conversations for the selected provider and exact project codebase, including sessions created outside Discord. Use `!attach <id>` to connect that channel to one of them. For Claude, the bridge searches `~/.claude/projects` for that codebase; for Codex, it queries the local app-server thread history filtered by the exact working directory. Attaching Claude starts it with `--resume`; attaching Codex uses `thread/resume`.

After a bridge restart, existing project channels are registered automatically. Use `!resume` in a project channel to resume that channel's saved conversation. The bridge persists the provider, model, and Claude/Codex conversation IDs in `~/.local/state/claude-discord-bridge/sessions.json`. Use `!sessions` and `!attach <id>` when you intentionally want to change the conversation attached to a channel. `connect` does not start a conversation; `start <project>` creates a new Claude session.

When Claude exits normally, the channel gets a notice with `!resume`. Send that command in the project channel to resume its recorded Claude conversation in the same codebase. If Claude was force-killed or the builder lost power before its SessionEnd hook ran, the notice may not be posted; `!resume` uses the conversation ID saved for that channel, falling back to local history only when no ID has been recorded yet.

The bot manages tmux and generates a temporary, session-only Claude Code hooks settings file automatically. You do not need to edit `~/.claude/settings.json`, manually create hooks, or launch tmux yourself. Plain Claude sessions you start directly over SSH remain independent and are not exposed to Discord.

## Choose provider and model

In a project's channel, use:

```text
!status
!sessions
!attach <session-or-thread-id>
!resume
!provider codex
!models
!model gpt-6-luna
!provider claude
!model opus
```

The bridge starts with Claude Code using `sonnet`. `!models` asks the installed Codex CLI for the models available to your signed-in account. For Claude, common model aliases are `default`, `sonnet`, `opus`, and `haiku`; you can also use a Claude model ID supported by your account. A model change applies to the next turn. Switching providers starts a fresh conversation in the same project channel because Claude and Codex keep separate conversation histories. To use Codex, install the Codex CLI and sign in as the same OS user that runs the service (`codex login status`). Provider and model selection persist per project across bridge restarts.

The bridge listens for hook callbacks only on `127.0.0.1:8787`; do not expose that port publicly.

## Discord controls and context

The bot registers guild slash commands: `/start`, `/list`, `/status`, `/stats`, `/pin`, `/unpin`, `/health`, `/resume`, `/interrupt`, `/stop`, `/sessions`, `/provider`, `/model`, `/git`, and `/cleanup`. Commands are restricted to IDs in `DISCORD_ALLOWED_USER_IDS`. `interrupt` sends Ctrl+C to Claude or requests an interrupt from the Codex app-server; `stop` stops the running process but keeps the channel and saved conversation available for `resume`.

Use `/pin` or `/unpin` (or `!pin` / `!unpin`) in a project channel to keep that project at the top of project lists. Pins persist across bridge restarts. `/health` or `!health` checks the builder's tmux, Claude and Codex CLIs, available project disk space, and user service status.

Requests containing common file-changing or Git operation terms are held for one hour and shown with **Approve once** and **Reject** buttons. Only the submitting user can choose. Approving sends the request to the assistant and adds it to that project's serialized turn queue. This is a keyword-based prompt gate: it does not inspect or replace Claude Code or Codex's own confirmations for tool actions. Turns in each project run one at a time, so messages sent while a turn is active wait in order.

Attach up to five supported files or images to a project-channel message to provide task context. Each file can be up to 10 MB, with a 20 MB combined limit. Files are downloaded from Discord's CDN into a temporary hidden directory inside the project (so Claude's project sandbox can read them) and removed after the assistant finishes. Supported types include text, JSON, YAML, TOML, CSV, PDF, and common image formats.

`/stats` or `!stats` reports whether the assistant is working, prompt count, last activity, and token totals parsed from Claude transcripts or Codex app-server usage notifications. If a Claude transcript contains a cost field, the summary includes it; Codex usage reports tokens, not a cost total. `/git` (or `!git status|diff`) shows repository state; `/git action:commit message:<message>` or `!git commit <message>` commits only changes that are already staged. The bridge never stages files for this command.

`/cleanup days:<days> stop_sessions:true` or `cleanup [days] [stop]` marks inactive project channels as archived in their topic. Project/channel mappings stay available; set `stop_sessions:true` (or add `stop` to the text command) to stop the old process too. The default inactivity threshold is 30 days. Archived channels are restored after a bridge restart and can be resumed normally.

Claude replies are posted when the session stop hook fires, with a completion notice. Codex turns post a completion notice and the final reply when the app-server reports the turn complete.

## Notes

- Only IDs in `DISCORD_ALLOWED_USER_IDS` can start sessions or send messages through the bridge.
- Project names are folder names directly under `PROJECTS_ROOT`, using letters, numbers, hyphens, and underscores.
- Discord channel creation requires Manage Channels. If channel creation fails, check that permission and the optional category ID.
- Channel/codebase mappings and provider/model/session selection are restored from project directories, Discord, and the bridge state file after a restart. `!resume` reconnects the conversation attached to that channel; use `start <project>` only when you want a new Claude session.
- Slack is not implemented in this build.
