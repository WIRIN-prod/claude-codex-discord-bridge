use anyhow::{anyhow, Context as _, Result};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use once_cell::sync::OnceCell;
use serde::Deserialize;
use serenity::{
    async_trait,
    builder::{
        CreateActionRow, CreateButton, CreateCommand, CreateCommandOption,
        CreateInteractionResponse, CreateInteractionResponseMessage, CreateMessage,
    },
    http::Http,
    model::application::{ButtonStyle, CommandOptionType, Interaction},
    model::{
        channel::{ChannelType, Message},
        gateway::Ready,
        id::{ChannelId, GuildId},
    },
    prelude::{Context, EventHandler, GatewayIntents},
    Client,
};
use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, RwLock},
};
use tracing::{error, info, warn};

static DISCORD_HTTP: OnceCell<Arc<Http>> = OnceCell::new();
static BOT_USER_ID: OnceCell<u64> = OnceCell::new();
type Shared = Arc<RwLock<AppState>>;

#[derive(Clone)]
struct Session {
    project: String,
    channel_id: u64,
    tmux_name: String,
    pane_id: String,
    transcript_path: Option<PathBuf>,
    claude_session_id: Option<String>,
    codex_thread_id: Option<String>,
    active: bool,
    provider: String,
    model: String,
    codex: Option<Arc<Mutex<CodexRuntime>>>,
    turns: u64,
    last_activity: u64,
    pending_files: Vec<PathBuf>,
    interrupt: Arc<tokio::sync::Notify>,
    busy: bool,
    input_tokens: u64,
    output_tokens: u64,
    cost_usd: Option<f64>,
    pinned: bool,
    turn_queue: Arc<Mutex<()>>,
    turn_done: Arc<tokio::sync::Notify>,
}

struct PendingApproval {
    project: String,
    requester_id: u64,
    prompt: String,
    files: Vec<PathBuf>,
    created_at: u64,
}

struct CodexRuntime {
    _child: Child,
    stdin: ChildStdin,
    stdout: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    thread_id: String,
    next_id: u64,
}

struct AppState {
    sessions: HashMap<String, Session>,
    pending_approvals: HashMap<String, PendingApproval>,
    persistence_path: PathBuf,
    persistence_lock: Arc<Mutex<()>>,
}

#[derive(Clone, Deserialize, serde::Serialize)]
struct StoredSession {
    project: String,
    provider: String,
    model: String,
    claude_session_id: Option<String>,
    codex_thread_id: Option<String>,
    #[serde(default)]
    turns: u64,
    #[serde(default)]
    last_activity: u64,
    #[serde(default)]
    busy: bool,
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
    #[serde(default)]
    cost_usd: Option<f64>,
    #[serde(default)]
    pinned: bool,
}

async fn persist_state(state: &Shared) -> Result<()> {
    let persistence_lock = state.read().await.persistence_lock.clone();
    let _write_guard = persistence_lock.lock().await;
    let (path, sessions) = {
        let guard = state.read().await;
        let stored = guard
            .sessions
            .values()
            .map(|s| StoredSession {
                project: s.project.clone(),
                provider: s.provider.clone(),
                model: s.model.clone(),
                claude_session_id: s.claude_session_id.clone(),
                codex_thread_id: s.codex_thread_id.clone(),
                turns: s.turns,
                last_activity: s.last_activity,
                busy: s.busy,
                input_tokens: s.input_tokens,
                output_tokens: s.output_tokens,
                cost_usd: s.cost_usd,
                pinned: s.pinned,
            })
            .collect::<Vec<_>>();
        (guard.persistence_path.clone(), stored)
    };
    let parent = path
        .parent()
        .context("state file has no parent directory")?;
    tokio::fs::create_dir_all(parent).await?;
    let tmp = path.with_extension(format!("json.{}.tmp", std::process::id()));
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(&sessions)?).await?;
    tokio::fs::rename(tmp, path).await?;
    Ok(())
}

#[derive(Clone)]
struct Config {
    token: String,
    secret: String,
    hook_url: String,
    projects_root: PathBuf,
    guild_id: u64,
    category_id: Option<u64>,
    allowed_users: Vec<u64>,
}

#[derive(Deserialize, serde::Serialize)]
struct HookEvent {
    hook_event_name: Option<String>,
    session_id: Option<String>,
    transcript_path: Option<PathBuf>,
    tmux_pane: Option<String>,
    #[serde(flatten)]
    _rest: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    if env::args().nth(1).as_deref() == Some("hook") {
        return hook_main().await;
    }

    let cfg = Arc::new(Config::from_env()?);
    let home = PathBuf::from(env::var("HOME").context("HOME is not set")?);
    let persistence_path = home.join(".local/state/claude-discord-bridge/sessions.json");
    let state: Shared = Arc::new(RwLock::new(AppState {
        sessions: HashMap::new(),
        pending_approvals: HashMap::new(),
        persistence_path,
        persistence_lock: Arc::new(Mutex::new(())),
    }));
    let http = Arc::new(Http::new(&cfg.token));
    DISCORD_HTTP
        .set(http)
        .map_err(|_| anyhow!("Discord HTTP already initialized"))?;

    let hook_state = state.clone();
    let hook_cfg = cfg.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/hooks/claude", post(receive_hook))
            .with_state((hook_state, hook_cfg));
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:8787").await {
            Ok(x) => x,
            Err(e) => {
                error!("hook listener failed: {e}");
                return;
            }
        };
        if let Err(e) = axum::serve(listener, app).await {
            error!("hook server failed: {e}");
        }
    });

    let intents = GatewayIntents::GUILDS
        | GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;
    let mut client = Client::builder(&cfg.token, intents)
        .event_handler(Handler { state, cfg })
        .await?;
    info!("Discord bridge online; DM `start <project>` to create or reconnect a project session");
    client.start().await?;
    Ok(())
}

impl Config {
    fn from_env() -> Result<Self> {
        let get = |key: &str| env::var(key).with_context(|| format!("missing {key} in .env"));
        let parse_id = |key: &str| -> Result<u64> {
            get(key)?
                .parse()
                .with_context(|| format!("{key} must be a numeric Discord ID"))
        };
        let allowed_users = get("DISCORD_ALLOWED_USER_IDS")?
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !allowed_users.is_empty(),
            "DISCORD_ALLOWED_USER_IDS must contain at least one numeric user ID"
        );
        Ok(Self {
            token: get("DISCORD_BOT_TOKEN")?,
            secret: get("CLAUDE_DISCORD_HOOK_SECRET")?,
            hook_url: env::var("CLAUDE_DISCORD_HOOK_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8787/hooks/claude".into()),
            projects_root: PathBuf::from(get("PROJECTS_ROOT")?),
            guild_id: parse_id("DISCORD_GUILD_ID")?,
            category_id: env::var("DISCORD_CATEGORY_ID")
                .ok()
                .and_then(|s| s.parse().ok()),
            allowed_users,
        })
    }
}

struct Handler {
    state: Shared,
    cfg: Arc<Config>,
}

async fn register_commands(cfg: &Config, http: &Http) -> Result<()> {
    let commands = vec![
        CreateCommand::new("start")
            .description("Start a new project session")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "project",
                    "Project directory under PROJECTS_ROOT",
                )
                .required(true),
            ),
        CreateCommand::new("list").description("List connected project sessions"),
        CreateCommand::new("status").description("Show this project's status"),
        CreateCommand::new("stats").description("Show activity and token usage"),
        CreateCommand::new("pin").description("Pin this project near the top of project lists"),
        CreateCommand::new("unpin").description("Remove this project pin"),
        CreateCommand::new("health").description("Check bridge tools and builder disk space"),
        CreateCommand::new("resume").description("Resume this channel's saved conversation"),
        CreateCommand::new("interrupt").description("Interrupt the current assistant turn"),
        CreateCommand::new("stop").description("Stop the session but keep its channel"),
        CreateCommand::new("sessions")
            .description("List existing provider conversations for this codebase"),
        CreateCommand::new("provider")
            .description("Choose Claude or Codex for this project")
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "provider", "Provider")
                    .required(true)
                    .add_string_choice("Claude", "claude")
                    .add_string_choice("Codex", "codex"),
            )
            .add_option(CreateCommandOption::new(
                CommandOptionType::String,
                "model",
                "Optional model name",
            )),
        CreateCommand::new("model")
            .description("Choose the model for this project")
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "model", "Model name")
                    .required(true),
            ),
        CreateCommand::new("git")
            .description("Inspect or commit staged project changes")
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "action", "Git action")
                    .required(true)
                    .add_string_choice("Status", "status")
                    .add_string_choice("Diff", "diff")
                    .add_string_choice("Commit staged changes", "commit"),
            )
            .add_option(CreateCommandOption::new(
                CommandOptionType::String,
                "message",
                "Commit message (for commit)",
            )),
        CreateCommand::new("cleanup")
            .description("Archive inactive channels; optionally stop old sessions")
            .add_option(CreateCommandOption::new(
                CommandOptionType::Integer,
                "days",
                "Minimum inactive age in days (default 30)",
            ))
            .add_option(CreateCommandOption::new(
                CommandOptionType::Boolean,
                "stop_sessions",
                "Stop old tmux/Codex sessions",
            )),
    ];
    GuildId::new(cfg.guild_id)
        .set_commands(http, commands)
        .await?;
    Ok(())
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn relative_time(timestamp: u64) -> String {
    if timestamp == 0 {
        return "unknown".into();
    }
    let seconds = unix_now().saturating_sub(timestamp);
    if seconds < 60 {
        "just now".into()
    } else if seconds < 3600 {
        format!("{}m ago", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h ago", seconds / 3600)
    } else {
        format!("{}d ago", seconds / 86_400)
    }
}

fn stats_summary(session: &Session) -> String {
    let usage = if session.input_tokens + session.output_tokens > 0 {
        format!(
            "{} input / {} output tokens",
            session.input_tokens, session.output_tokens
        )
    } else {
        "provider token totals unavailable".into()
    };
    let cost = session
        .cost_usd
        .map(|x| format!(" · transcript cost ${x:.4}"))
        .unwrap_or_default();
    format!(
        "`{}` · {} · {} turns · last activity {} · {}{}",
        session.project,
        if session.busy { "working" } else { "idle" },
        session.turns,
        relative_time(session.last_activity),
        usage,
        cost
    )
}

fn transcript_usage(path: Option<&Path>) -> (u64, u64, Option<f64>) {
    let Some(content) = path.and_then(|p| std::fs::read_to_string(p).ok()) else {
        return (0, 0, None);
    };
    let (mut input, mut output, mut cost) = (0, 0, 0.0);
    let mut has_cost = false;
    for line in content.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let usage = value
            .pointer("/message/usage")
            .or_else(|| value.get("usage"));
        if let Some(usage) = usage {
            input += usage
                .get("input_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                + usage
                    .get("cache_read_input_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0)
                + usage
                    .get("cache_creation_input_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
            output += usage
                .get("output_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
        }
        if let Some(value) = value
            .get("costUSD")
            .or_else(|| value.get("cost_usd"))
            .and_then(|v| v.as_f64())
        {
            cost += value;
            has_cost = true;
        }
    }
    (input, output, has_cost.then_some(cost))
}

fn risky_prompt(prompt: &str) -> bool {
    let lowered = prompt.to_ascii_lowercase();
    let words = lowered
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    [
        "edit",
        "write",
        "modify",
        "change",
        "create",
        "delete",
        "remove",
        "install",
        "commit",
        "push",
        "reset",
        "rebase",
        "deploy",
        "overwrite",
        "rename",
        "move",
    ]
    .iter()
    .any(|word| words.contains(word))
}

async fn health_command(cfg: &Config) -> String {
    async fn check(program: String, arg: &'static str) -> String {
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            Command::new(&program).arg(arg).output(),
        )
        .await
        {
            Ok(Ok(output)) if output.status.success() => {
                let line = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .next()
                    .unwrap_or("available")
                    .trim()
                    .to_owned();
                format!("ok ({line})")
            }
            Ok(Ok(output)) => format!("error ({})", String::from_utf8_lossy(&output.stderr).trim()),
            Ok(Err(e)) => format!("unavailable ({e})"),
            Err(_) => "timed out".into(),
        }
    }
    let tmux = check("tmux".into(), "-V").await;
    let claude = check(
        env::var("CLAUDE_COMMAND").unwrap_or_else(|_| "claude".into()),
        "--version",
    )
    .await;
    let codex = check(
        env::var("CODEX_COMMAND").unwrap_or_else(|_| "codex".into()),
        "--version",
    )
    .await;
    let disk = match Command::new("df")
        .args(["-Pk", cfg.projects_root.to_string_lossy().as_ref()])
        .output()
        .await
    {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout)
            .lines()
            .nth(1)
            .and_then(|line| {
                let columns = line.split_whitespace().collect::<Vec<_>>();
                Some(format!(
                    "{} free · {} used",
                    human_bytes(columns.get(3)?.parse::<u64>().ok()?.saturating_mul(1024)),
                    columns.get(4)?
                ))
            })
            .unwrap_or_else(|| "unavailable".into()),
        _ => "unavailable".into(),
    };
    let service = match Command::new("systemctl")
        .args(["--user", "is-active", "claude-discord-bridge.service"])
        .output()
        .await
    {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        }
        _ => "inactive / not installed".into(),
    };
    format!("Builder health\nBridge: running\ntmux: {tmux}\nClaude CLI: {claude}\nCodex CLI: {codex}\nProject disk: {disk}\nUser service: {service}")
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

async fn set_pinned(state: &Shared, project: &str, pinned: bool) -> Result<()> {
    let mut guard = state.write().await;
    let session = guard
        .sessions
        .get_mut(project)
        .context("project session is no longer registered")?;
    session.pinned = pinned;
    drop(guard);
    persist_state(state).await
}

async fn download_attachments(msg: &Message, project_dir: &Path) -> Result<Vec<PathBuf>> {
    anyhow::ensure!(
        msg.attachments.len() <= 5,
        "at most five attachments can be sent in one message"
    );
    let root = project_dir
        .join(".bridge-attachments")
        .join(msg.id.get().to_string());
    tokio::fs::create_dir_all(&root).await?;
    let client = reqwest::Client::new();
    let mut paths = Vec::new();
    let mut total = 0usize;
    for (index, attachment) in msg.attachments.iter().enumerate() {
        let url = reqwest::Url::parse(&attachment.url)?;
        anyhow::ensure!(
            url.scheme() == "https"
                && url
                    .host_str()
                    .is_some_and(|h| h == "cdn.discordapp.com" || h == "media.discordapp.net"),
            "attachment URL is not a Discord CDN URL"
        );
        let extension = Path::new(&attachment.filename)
            .extension()
            .and_then(|x| x.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        anyhow::ensure!(
            [
                "txt", "md", "rst", "json", "yaml", "yml", "toml", "csv", "log", "pdf", "png",
                "jpg", "jpeg", "gif", "webp", "svg"
            ]
            .contains(&extension.as_str()),
            "unsupported attachment type `{extension}`"
        );
        anyhow::ensure!(
            attachment.size <= 10 * 1024 * 1024,
            "each attachment must be 10 MB or smaller"
        );
        total += attachment.size as usize;
        anyhow::ensure!(
            total <= 20 * 1024 * 1024,
            "combined attachments must be 20 MB or smaller"
        );
        let response = client.get(url).send().await?.error_for_status()?;
        anyhow::ensure!(
            response
                .content_length()
                .is_none_or(|size| size <= 10 * 1024 * 1024),
            "downloaded attachment exceeds 10 MB"
        );
        let bytes = response.bytes().await?;
        anyhow::ensure!(
            bytes.len() <= 10 * 1024 * 1024,
            "downloaded attachment exceeds 10 MB"
        );
        total = total
            .saturating_sub(attachment.size as usize)
            .saturating_add(bytes.len());
        anyhow::ensure!(
            total <= 20 * 1024 * 1024,
            "downloaded attachments exceed 20 MB combined"
        );
        let name = format!("{index}.{}", extension);
        let path = root.join(name);
        tokio::fs::write(&path, &bytes).await?;
        paths.push(path);
    }
    Ok(paths)
}

async fn remove_temp_files(paths: Vec<PathBuf>) {
    for path in paths {
        let parent = path.parent().map(Path::to_path_buf);
        let _ = tokio::fs::remove_file(path).await;
        if let Some(parent) = parent {
            let _ = tokio::fs::remove_dir(parent).await;
        }
    }
}

async fn stop_project(state: &Shared, session: &Session) -> Result<()> {
    if session.provider == "codex" && session.busy {
        let _ = interrupt_project(session).await;
    }
    if let Some(codex) = &session.codex {
        let mut runtime = codex.lock().await;
        let _ = runtime._child.start_kill();
    }
    if !session.tmux_name.is_empty() && pane_exists(&session.tmux_name).await {
        let status = Command::new("tmux")
            .args(["kill-session", "-t", &session.tmux_name])
            .status()
            .await?;
        anyhow::ensure!(status.success(), "tmux could not stop the session");
    }
    let mut guard = state.write().await;
    let pending = guard
        .sessions
        .get(&session.project)
        .map(|s| s.pending_files.clone())
        .unwrap_or_default();
    if let Some(s) = guard.sessions.get_mut(&session.project) {
        s.active = false;
        s.busy = false;
        s.codex = None;
        s.pending_files.clear();
    }
    drop(guard);
    remove_temp_files(pending).await;
    persist_state(state).await
}

async fn interrupt_project(session: &Session) -> Result<()> {
    anyhow::ensure!(
        session.busy,
        "there is no active assistant turn to interrupt"
    );
    if session.provider == "codex" {
        session.interrupt.notify_one();
        return Ok(());
    }
    anyhow::ensure!(
        pane_exists(&session.tmux_name).await,
        "Claude tmux session is not running"
    );
    let status = Command::new("tmux")
        .args(["send-keys", "-t", &session.pane_id, "C-c"])
        .status()
        .await?;
    anyhow::ensure!(status.success(), "tmux could not send Ctrl+C");
    Ok(())
}

async fn git_action(
    cfg: &Config,
    session: &Session,
    action: &str,
    message: Option<&str>,
) -> Result<String> {
    anyhow::ensure!(
        action == "status" || action == "diff" || action == "commit",
        "git action must be `status`, `diff`, or `commit`"
    );
    let cwd = cfg.projects_root.join(&session.project);
    if action == "commit" {
        let message = message
            .context("usage: `!git commit <message>` (commits only already-staged changes)")?;
        anyhow::ensure!(!message.trim().is_empty(), "commit message cannot be empty");
        let output = Command::new("git")
            .args(["commit", "-m", message])
            .current_dir(cwd)
            .output()
            .await?;
        anyhow::ensure!(
            output.status.success(),
            "git commit failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned());
    }
    let output = Command::new("git")
        .args(if action == "diff" {
            vec!["diff", "HEAD", "--no-ext-diff", "--unified=2"]
        } else {
            vec!["status", "--short", "--branch"]
        })
        .current_dir(cwd)
        .output()
        .await?;
    anyhow::ensure!(
        output.status.success(),
        "git {action} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    Ok(if text.is_empty() {
        "Working tree is clean.".into()
    } else {
        format!(
            "```text\n{}\n```",
            text.chars().take(1800).collect::<String>()
        )
    })
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: Ready) {
        let _ = BOT_USER_ID.set(ready.user.id.get());
        info!(user = %ready.user.name, "connected to Discord");
        if let Err(e) = register_commands(&self.cfg, &ctx.http).await {
            warn!("could not register Discord slash commands: {e:#}");
        }
        if let Err(e) = self.restore_sessions(&ctx).await {
            warn!("could not restore existing tmux sessions: {e:#}");
        }
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        let command = match interaction {
            Interaction::Command(command) => command,
            Interaction::Component(component) => {
                self.handle_approval(&ctx, component).await;
                return;
            }
            _ => return,
        };
        if !self.cfg.allowed_users.contains(&command.user.id.get()) {
            let _ = command
                .create_response(
                    &ctx.http,
                    CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .content("You are not allowed to use this bridge.")
                            .ephemeral(true),
                    ),
                )
                .await;
            return;
        }
        if command.defer_ephemeral(&ctx.http).await.is_err() {
            return;
        }
        let action = command.data.name.as_str();
        let args = command.data.options.as_slice();
        let arg = |name: &str| -> Option<String> {
            args.iter()
                .find(|o| o.name == name)
                .and_then(|o| match &o.value {
                    serenity::model::application::CommandDataOptionValue::String(s) => {
                        Some(s.clone())
                    }
                    serenity::model::application::CommandDataOptionValue::Integer(i) => {
                        Some(i.to_string())
                    }
                    _ => None,
                })
        };
        let bool_arg = |name: &str| -> bool {
            args.iter().find(|o| o.name == name).is_some_and(|o| {
                matches!(
                    o.value,
                    serenity::model::application::CommandDataOptionValue::Boolean(true)
                )
            })
        };
        let result: Result<String> = match action {
            "start" => {
                let project = arg("project").unwrap_or_default();
                start_project(&ctx, &self.cfg, &self.state, &project)
                    .await
                    .map(|ch| format!("Started `{project}` in <#{ch}>."))
            }
            "list" => {
                let state = self.state.read().await;
                let mut sessions = state.sessions.values().collect::<Vec<_>>();
                sessions.sort_by(|a, b| {
                    b.pinned
                        .cmp(&a.pinned)
                        .then_with(|| a.project.cmp(&b.project))
                });
                Ok(if sessions.is_empty() {
                    "No project sessions started yet.".into()
                } else {
                    sessions
                        .into_iter()
                        .map(|s| {
                            format!(
                                "{} `{}` <#{}> · {} · {} turns",
                                if s.pinned { "📌" } else { "•" },
                                s.project,
                                s.channel_id,
                                if s.busy { "working" } else { "idle" },
                                s.turns
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
            }
            "health" => Ok(health_command(&self.cfg).await),
            "cleanup" => {
                self.cleanup_sessions(
                    arg("days").and_then(|x| x.parse().ok()).unwrap_or(30),
                    bool_arg("stop_sessions"),
                    &ctx,
                )
                .await
            }
            "provider" => {
                let provider = arg("provider").unwrap_or_default();
                let model = arg("model").unwrap_or_else(|| {
                    if provider == "codex" {
                        "default".into()
                    } else {
                        "sonnet".into()
                    }
                });
                let current = self
                    .state
                    .read()
                    .await
                    .sessions
                    .values()
                    .find(|s| s.channel_id == command.channel_id.get())
                    .cloned();
                match current {
                    Some(s) => switch_provider(&self.cfg, &self.state, &s, &provider, &model)
                        .await
                        .map(|_| {
                            format!(
                                "`{}` now uses `{provider}` with model `{model}`.",
                                s.project
                            )
                        }),
                    None => Err(anyhow!("run this command in a project channel")),
                }
            }
            "model" => {
                let model = arg("model").unwrap_or_default();
                let current = self
                    .state
                    .read()
                    .await
                    .sessions
                    .values()
                    .find(|s| s.channel_id == command.channel_id.get())
                    .cloned();
                match current {
                    Some(s) => change_model(&self.state, &s, &model)
                        .await
                        .map(|_| format!("`{}` will use `{model}` on its next turn.", s.project)),
                    None => Err(anyhow!("run this command in a project channel")),
                }
            }
            other => {
                let current = self
                    .state
                    .read()
                    .await
                    .sessions
                    .values()
                    .find(|s| s.channel_id == command.channel_id.get())
                    .cloned();
                match current {
                    None => Err(anyhow!("run this command in a project channel")),
                    Some(s) => match other {
                        "pin" => set_pinned(&self.state, &s.project, true).await.map(|_| format!("Pinned `{}` in project lists.", s.project)),
                        "unpin" => set_pinned(&self.state, &s.project, false).await.map(|_| format!("Unpinned `{}`.", s.project)),
                        "status" => Ok(format!("`{}` · {} · model `{}` · {} · {} turns", s.project, s.provider, s.model, if s.busy { "working" } else { "idle" }, s.turns)),
                        "stats" => Ok(stats_summary(&s)),
                        "resume" => resume_session(&self.cfg, &self.state, &s).await.map(|_| format!("Resumed `{}`.", s.project)),
                        "stop" => stop_project(&self.state, &s).await.map(|_| format!("Stopped `{}`; its channel remains available for resume.", s.project)),
                        "interrupt" => interrupt_project(&s).await.map(|_| format!("Sent an interrupt to `{}`.", s.project)),
                        "sessions" => if s.provider == "codex" { list_codex_sessions(&self.cfg, &s).await } else { list_claude_sessions(&self.cfg, &s.project).await },
                        "git" => git_action(&self.cfg, &s, &arg("action").unwrap_or_else(|| "status".into()), arg("message").as_deref()).await,
                        _ => Ok("Use `/start` to create a session, then `/resume`, `/provider`, `/model`, `/sessions`, `/interrupt`, `/stop`, `/stats`, `/git`, or `/cleanup` in a project channel.".into()),
                    }
                }
            }
        };
        let content = result.unwrap_or_else(|e| format!("Bridge command failed: {e:#}"));
        let _ = command
            .edit_response(
                &ctx.http,
                serenity::builder::EditInteractionResponse::new().content(content),
            )
            .await;
    }

    async fn message(&self, ctx: Context, msg: Message) {
        info!(author = msg.author.id.get(), channel = msg.channel_id.get(), guild = ?msg.guild_id.map(|id| id.get()), "received Discord message event");
        if msg.author.bot || !self.cfg.allowed_users.contains(&msg.author.id.get()) {
            return;
        }
        let is_dm = msg.guild_id.is_none();
        let mentioned_here = !is_dm
            && msg.guild_id.map(|id| id.get()) == Some(self.cfg.guild_id)
            && (BOT_USER_ID
                .get()
                .is_some_and(|id| msg.mentions.iter().any(|user| user.id.get() == *id))
                || !msg.mention_roles.is_empty());
        if is_dm || mentioned_here {
            let mut command = msg.content.clone();
            if mentioned_here {
                if let Some(id) = BOT_USER_ID.get() {
                    command = command
                        .replace(&format!("<@{id}>",), "")
                        .replace(&format!("<@!{id}>",), "");
                }
                for role in &msg.mention_roles {
                    command = command.replace(&format!("<@&{}>", role.get()), "");
                }
            }
            let mut words = command.split_whitespace();
            match words.next().unwrap_or("").to_lowercase().as_str() {
                "start" => {
                    let project = words.next().unwrap_or("");
                    if project.is_empty() {
                        let _ = msg
                            .channel_id
                            .say(&ctx.http, "Usage: `start <project-directory>`")
                            .await;
                        return;
                    }
                    match start_project(&ctx, &self.cfg, &self.state, project).await {
                        Ok(ch) => {
                            let _ = msg
                                .channel_id
                                .say(
                                    &ctx.http,
                                    format!(
                                        "Ready: <#{ch}>. Send Claude messages in that channel."
                                    ),
                                )
                                .await;
                        }
                        Err(e) => {
                            warn!("start failed: {e:#}");
                            let _ = msg
                                .channel_id
                                .say(&ctx.http, format!("Could not start `{project}`: {e:#}"))
                                .await;
                        }
                    }
                    return;
                }
                "connect" => {
                    let project = words.next().unwrap_or("");
                    let provider = words.next().unwrap_or("claude");
                    match connect_project(&ctx, &self.cfg, &self.state, project, provider).await {
                        Ok(ch) => {
                            let _ = msg.channel_id.say(&ctx.http, format!("Connected project `{project}` to its existing channel <#{ch}> without starting a conversation. Use `!sessions`, then `!attach <id>` there.")).await;
                        }
                        Err(e) => {
                            let _ = msg
                                .reply(&ctx.http, format!("Could not connect project: {e:#}"))
                                .await;
                        }
                    }
                    return;
                }
                "list" => {
                    let s = self.state.read().await;
                    let mut sessions = s.sessions.values().collect::<Vec<_>>();
                    sessions.sort_by(|a, b| {
                        b.pinned
                            .cmp(&a.pinned)
                            .then_with(|| a.project.cmp(&b.project))
                    });
                    let out = if sessions.is_empty() {
                        "No project sessions started yet.".into()
                    } else {
                        sessions
                            .into_iter()
                            .map(|x| {
                                format!(
                                    "{} `{}` <#{}>",
                                    if x.pinned { "📌" } else { "•" },
                                    x.project,
                                    x.channel_id
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    };
                    let _ = msg.channel_id.say(&ctx.http, out).await;
                    return;
                }
                "health" => {
                    let _ = msg
                        .channel_id
                        .say(&ctx.http, health_command(&self.cfg).await)
                        .await;
                    return;
                }
                "cleanup" => {
                    let days = words
                        .next()
                        .and_then(|x| x.parse::<u64>().ok())
                        .unwrap_or(30);
                    let stop_sessions = words.any(|word| word == "stop");
                    match self.cleanup_sessions(days, stop_sessions, &ctx).await {
                        Ok(text) => {
                            let _ = msg.channel_id.say(&ctx.http, text).await;
                        }
                        Err(e) => {
                            let _ = msg.reply(&ctx.http, format!("Cleanup failed: {e:#}")).await;
                        }
                    }
                    return;
                }
                "help" => {
                    let _ = msg.channel_id.say(&ctx.http, "Slash commands: `/start`, `/list`, `/status`, `/stats`, `/pin`, `/unpin`, `/health`, `/resume`, `/interrupt`, `/stop`, `/sessions`, `/provider`, `/model`, `/git`, `/cleanup`. Text commands: `!interrupt`, `!stop`, `!stats`, `!pin`, `!unpin`, `!health`, `!git status|diff|commit <message>`, and `!cleanup [days] [stop]` in project channels. Risky requests may ask for approval before they are sent. Attach supported files or images to a message to send them as context.").await;
                    return;
                }
                _ if is_dm => {
                    let _ = msg.channel_id.say(&ctx.http, "Use `/start`, `/list`, or `/cleanup`; or use `start <project-directory>`, `connect <project> [claude|codex]`, `list`, `cleanup [days] [stop]` in a DM or bot mention.").await;
                    return;
                }
                _ => {}
            }
        }
        let session = {
            let state = self.state.read().await;
            state
                .sessions
                .values()
                .find(|s| s.channel_id == msg.channel_id.get())
                .cloned()
        };
        if let Some(session) = session {
            let mut words = msg.content.split_whitespace();
            match words.next().unwrap_or("") {
                "!provider" => {
                    let provider = words.next().unwrap_or("");
                    let model = words.next().map(str::to_owned).unwrap_or_else(|| {
                        if provider == "codex" {
                            "default".into()
                        } else {
                            "sonnet".into()
                        }
                    });
                    match switch_provider(&self.cfg, &self.state, &session, provider, &model).await
                    {
                        Ok(()) => {
                            let _ = msg.channel_id.say(&ctx.http, format!("`{}` now uses **{}** with model `{}`. Provider changes start a fresh AI conversation in this channel.", session.project, provider, model)).await;
                        }
                        Err(e) => {
                            error!("provider switch failed: {e:#}");
                            let _ = msg
                                .reply(&ctx.http, format!("Could not switch provider: {e:#}"))
                                .await;
                        }
                    }
                }
                "!model" => {
                    let model = words.next().unwrap_or("");
                    match change_model(&self.state, &session, model).await {
                        Ok(()) => {
                            let _ = msg
                                .channel_id
                                .say(
                                    &ctx.http,
                                    format!(
                                        "`{}` will use model `{}` on its next turn.",
                                        session.project, model
                                    ),
                                )
                                .await;
                        }
                        Err(e) => {
                            let _ = msg
                                .reply(&ctx.http, format!("Could not change model: {e:#}"))
                                .await;
                        }
                    }
                }
                "!models" => {
                    let result: Result<String> = if session.provider == "codex" {
                        if let Some(c) = &session.codex {
                            c.lock().await.models().await.map(|m| m.join(", "))
                        } else {
                            Err(anyhow!("Codex app-server is not connected"))
                        }
                    } else {
                        Ok("default, sonnet, opus, haiku (Claude model aliases)".into())
                    };
                    match result {
                        Ok(m) => {
                            let _ = msg
                                .channel_id
                                .say(
                                    &ctx.http,
                                    format!("Available `{}` models: {}", session.provider, m),
                                )
                                .await;
                        }
                        Err(e) => {
                            let _ = msg
                                .reply(&ctx.http, format!("Could not list models: {e:#}"))
                                .await;
                        }
                    }
                }
                "!status" => {
                    let codebase = self.cfg.projects_root.join(&session.project);
                    let _ = msg
                        .channel_id
                        .say(
                            &ctx.http,
                            format!(
                                "Project `{}` · codebase `{}` · provider `{}` · model `{}` · {}",
                                session.project,
                                codebase.display(),
                                session.provider,
                                session.model,
                                if session.active {
                                    "active"
                                } else {
                                    "inactive; use `!resume`"
                                }
                            ),
                        )
                        .await;
                }
                "!interrupt" => match interrupt_project(&session).await {
                    Ok(()) => {
                        let _ = msg
                            .channel_id
                            .say(&ctx.http, "Sent an interrupt to the active assistant turn.")
                            .await;
                    }
                    Err(e) => {
                        let _ = msg
                            .reply(&ctx.http, format!("Could not interrupt: {e:#}"))
                            .await;
                    }
                },
                "!stop" => match stop_project(&self.state, &session).await {
                    Ok(()) => {
                        let _ = msg.channel_id.say(&ctx.http, "Stopped the session. The project channel remains available; use `!resume` to continue later.").await;
                    }
                    Err(e) => {
                        let _ = msg
                            .reply(&ctx.http, format!("Could not stop session: {e:#}"))
                            .await;
                    }
                },
                "!stats" => {
                    let _ = msg.channel_id.say(&ctx.http, stats_summary(&session)).await;
                }
                "!pin" => match set_pinned(&self.state, &session.project, true).await {
                    Ok(()) => {
                        let _ = msg
                            .channel_id
                            .say(
                                &ctx.http,
                                format!("Pinned `{}` in project lists.", session.project),
                            )
                            .await;
                    }
                    Err(e) => {
                        let _ = msg
                            .reply(&ctx.http, format!("Could not pin project: {e:#}"))
                            .await;
                    }
                },
                "!unpin" => match set_pinned(&self.state, &session.project, false).await {
                    Ok(()) => {
                        let _ = msg
                            .channel_id
                            .say(&ctx.http, format!("Unpinned `{}`.", session.project))
                            .await;
                    }
                    Err(e) => {
                        let _ = msg
                            .reply(&ctx.http, format!("Could not unpin project: {e:#}"))
                            .await;
                    }
                },
                "!cleanup" => {
                    let days = words
                        .next()
                        .and_then(|x| x.parse::<u64>().ok())
                        .unwrap_or(30);
                    let stop_sessions = words.any(|word| word == "stop");
                    match self.cleanup_sessions(days, stop_sessions, &ctx).await {
                        Ok(text) => {
                            let _ = msg.channel_id.say(&ctx.http, text).await;
                        }
                        Err(e) => {
                            let _ = msg.reply(&ctx.http, format!("Cleanup failed: {e:#}")).await;
                        }
                    }
                }
                "!git" => {
                    let action = words.next().unwrap_or("status");
                    let message = words.collect::<Vec<_>>().join(" ");
                    match git_action(
                        &self.cfg,
                        &session,
                        action,
                        if message.is_empty() {
                            None
                        } else {
                            Some(&message)
                        },
                    )
                    .await
                    {
                        Ok(text) => {
                            let _ = msg.channel_id.say(&ctx.http, text).await;
                        }
                        Err(e) => {
                            let _ = msg
                                .reply(&ctx.http, format!("Git helper failed: {e:#}"))
                                .await;
                        }
                    }
                }
                "!resume" => {
                    let result = resume_session(&self.cfg, &self.state, &session).await;
                    match result {
                        Ok(()) => {
                            let _ = msg.channel_id.say(&ctx.http, format!("Resuming the latest `{}` conversation for `{}` in its linked codebase.", session.provider, session.project)).await;
                        }
                        Err(e) => {
                            let _ = msg
                                .reply(&ctx.http, format!("Could not resume session: {e:#}"))
                                .await;
                        }
                    }
                }
                "!sessions" => {
                    let result: Result<String> = if session.provider == "codex" {
                        list_codex_sessions(&self.cfg, &session).await
                    } else {
                        list_claude_sessions(&self.cfg, &session.project).await
                    };
                    match result {
                        Ok(items) => {
                            for part in discord_chunks(&items) {
                                let _ = msg.channel_id.say(&ctx.http, part).await;
                            }
                        }
                        Err(e) => {
                            let _ = msg
                                .reply(
                                    &ctx.http,
                                    format!("Could not list existing sessions: {e:#}"),
                                )
                                .await;
                        }
                    }
                }
                "!attach" => {
                    let id = words.next().unwrap_or("");
                    if id.is_empty() {
                        let _ = msg.reply(&ctx.http, "Usage: `!attach <session-or-thread-id>` (use `!sessions` to list matching conversations)").await;
                    } else {
                        match attach_existing(&self.cfg, &self.state, &session, id).await {
                            Ok(()) => {
                                let _ = msg
                                    .channel_id
                                    .say(
                                        &ctx.http,
                                        format!(
                                            "Attached `{}` to `{}` in this project's codebase.",
                                            session.provider, session.project
                                        ),
                                    )
                                    .await;
                            }
                            Err(e) => {
                                let _ = msg
                                    .reply(
                                        &ctx.http,
                                        format!("Could not attach conversation: {e:#}"),
                                    )
                                    .await;
                            }
                        }
                    }
                }
                _ => {
                    let mut prompt = msg.content.clone();
                    let mut attached = Vec::new();
                    if !msg.attachments.is_empty() {
                        let project_dir = self.cfg.projects_root.join(&session.project);
                        match download_attachments(&msg, &project_dir).await {
                            Ok(files) => {
                                if !files.is_empty() {
                                    prompt.push_str("\n\nDiscord attachments (read these files as task context):");
                                    for path in &files {
                                        prompt.push_str(&format!("\n- {}", path.display()));
                                    }
                                    attached = files;
                                }
                            }
                            Err(e) => {
                                let _ = msg
                                    .reply(
                                        &ctx.http,
                                        format!("Could not retrieve attachment(s): {e:#}"),
                                    )
                                    .await;
                                return;
                            }
                        }
                    }
                    if risky_prompt(&prompt) {
                        let key = msg.id.get().to_string();
                        let mut state = self.state.write().await;
                        let expired = state
                            .pending_approvals
                            .iter()
                            .filter(|(_, pending)| {
                                unix_now().saturating_sub(pending.created_at) > 3600
                            })
                            .map(|(id, _)| id.clone())
                            .collect::<Vec<_>>();
                        let mut expired_files = Vec::new();
                        for id in expired {
                            if let Some(pending) = state.pending_approvals.remove(&id) {
                                expired_files.extend(pending.files);
                            }
                        }
                        if state.pending_approvals.len() >= 100 {
                            drop(state);
                            remove_temp_files(expired_files).await;
                            remove_temp_files(attached).await;
                            let _ = msg.reply(&ctx.http, "Too many approval requests are waiting. Resolve one and retry.").await;
                            return;
                        }
                        state.pending_approvals.insert(
                            key.clone(),
                            PendingApproval {
                                project: session.project.clone(),
                                requester_id: msg.author.id.get(),
                                prompt,
                                files: attached.clone(),
                                created_at: unix_now(),
                            },
                        );
                        drop(state);
                        remove_temp_files(expired_files).await;
                        let approval = CreateMessage::new()
                            .content("This request may change project files, install software, or alter Git history. Approve sending it to the assistant?")
                            .components(vec![CreateActionRow::Buttons(vec![
                                CreateButton::new(format!("approve:{key}"))
                                    .label("Approve once")
                                    .style(ButtonStyle::Success),
                                CreateButton::new(format!("reject:{key}"))
                                    .label("Reject")
                                    .style(ButtonStyle::Danger),
                            ])]);
                        if let Err(e) = msg.channel_id.send_message(&ctx.http, approval).await {
                            self.state.write().await.pending_approvals.remove(&key);
                            remove_temp_files(attached).await;
                            error!("could not send approval controls: {e}");
                        }
                    } else {
                        self.process_turn(&ctx, msg.channel_id, session, prompt, attached)
                            .await;
                    }
                }
            }
        }
    }
}

impl Handler {
    async fn handle_approval(
        &self,
        ctx: &Context,
        component: serenity::model::application::ComponentInteraction,
    ) {
        let Some((action, key)) = component.data.custom_id.split_once(':') else {
            return;
        };
        if !self.cfg.allowed_users.contains(&component.user.id.get()) {
            let _ = component
                .create_response(
                    &ctx.http,
                    CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .content("You are not allowed to use this bridge.")
                            .ephemeral(true),
                    ),
                )
                .await;
            return;
        }
        let mut state = self.state.write().await;
        let Some(pending) = state.pending_approvals.get(key) else {
            drop(state);
            let _ = component
                .create_response(
                    &ctx.http,
                    CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .content("This approval request is no longer available.")
                            .ephemeral(true),
                    ),
                )
                .await;
            return;
        };
        if unix_now().saturating_sub(pending.created_at) > 3600 {
            let expired = state.pending_approvals.remove(key).unwrap();
            drop(state);
            remove_temp_files(expired.files).await;
            let _ = component
                .create_response(
                    &ctx.http,
                    CreateInteractionResponse::UpdateMessage(
                        CreateInteractionResponseMessage::new()
                            .content(
                                "This approval request expired. Send the request again to retry.",
                            )
                            .components(Vec::new()),
                    ),
                )
                .await;
            return;
        }
        if pending.requester_id != component.user.id.get() {
            drop(state);
            let _ = component
                .create_response(
                    &ctx.http,
                    CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .content("Only the person who submitted this request can approve it.")
                            .ephemeral(true),
                    ),
                )
                .await;
            return;
        }
        let pending = state.pending_approvals.remove(key).unwrap();
        let session = state.sessions.get(&pending.project).cloned();
        drop(state);
        let approving = action == "approve";
        let response = if approving {
            format!(
                "Approved by <@{}>. The request is queued.",
                component.user.id.get()
            )
        } else {
            "Request rejected; it was not sent to the assistant.".to_owned()
        };
        let _ = component
            .create_response(
                &ctx.http,
                CreateInteractionResponse::UpdateMessage(
                    CreateInteractionResponseMessage::new()
                        .content(response)
                        .components(Vec::new()),
                ),
            )
            .await;
        if approving {
            if let Some(session) = session {
                self.process_turn(
                    ctx,
                    component.channel_id,
                    session,
                    pending.prompt,
                    pending.files,
                )
                .await;
            } else {
                remove_temp_files(pending.files).await;
            }
        } else {
            remove_temp_files(pending.files).await;
        }
    }

    async fn process_turn(
        &self,
        ctx: &Context,
        channel_id: ChannelId,
        session: Session,
        prompt: String,
        attached: Vec<PathBuf>,
    ) {
        if session.busy {
            let _ = channel_id
                .say(&ctx.http, "Your request is queued behind the current turn.")
                .await;
        }
        let _turn_guard = session.turn_queue.lock().await;
        {
            let mut state = self.state.write().await;
            if let Some(s) = state.sessions.get_mut(&session.project) {
                s.busy = true;
                s.last_activity = unix_now();
                if s.provider == "codex" {
                    s.turns = s.turns.saturating_add(1);
                    s.active = true;
                } else {
                    s.pending_files.extend(attached.iter().cloned());
                }
            }
        }
        if session.provider == "codex" {
            let _ = persist_state(&self.state).await;
        }
        let _ = channel_id.say(&ctx.http, "Working on it…").await;
        let result = if session.provider == "codex" {
            if let Some(codex) = &session.codex {
                codex
                    .lock()
                    .await
                    .turn(&prompt, &session.model, &session.interrupt)
                    .await
                    .map(Some)
            } else {
                Err(anyhow!("Codex app-server is not connected"))
            }
        } else {
            send_to_claude(&session, &prompt).await.map(|_| None)
        };
        if session.provider == "codex" {
            if let Some(s) = self.state.write().await.sessions.get_mut(&session.project) {
                s.active = false;
                s.busy = false;
            }
        }
        match result {
            Ok(Some((answer, input_tokens, output_tokens))) => {
                if let Some(s) = self.state.write().await.sessions.get_mut(&session.project) {
                    s.input_tokens = input_tokens;
                    s.output_tokens = output_tokens;
                }
                let _ = persist_state(&self.state).await;
                let _ = channel_id.say(&ctx.http, "Task finished.").await;
                for part in discord_chunks(&answer) {
                    let _ = channel_id.say(&ctx.http, part).await;
                }
                remove_temp_files(attached).await;
            }
            Ok(None) => {
                if session.provider == "claude" {
                    let notified = session.turn_done.notified();
                    if tokio::time::timeout(std::time::Duration::from_secs(900), notified)
                        .await
                        .is_err()
                    {
                        let mut state = self.state.write().await;
                        if let Some(s) = state.sessions.get_mut(&session.project) {
                            s.pending_files.retain(|path| !attached.contains(path));
                            s.busy = false;
                        }
                        drop(state);
                        remove_temp_files(attached).await;
                        let _ = persist_state(&self.state).await;
                        let _ = channel_id.say(&ctx.http, "No completion hook arrived within 15 minutes. The queued turn was released; use `!status` to check the session.").await;
                    }
                }
            }
            Err(e) => {
                remove_temp_files(attached.clone()).await;
                if session.provider == "claude" {
                    let mut state = self.state.write().await;
                    if let Some(s) = state.sessions.get_mut(&session.project) {
                        s.pending_files.retain(|path| !attached.contains(path));
                        s.busy = false;
                    }
                } else {
                    if let Some(s) = self.state.write().await.sessions.get_mut(&session.project) {
                        s.busy = false;
                    }
                    let _ = persist_state(&self.state).await;
                }
                error!("input delivery failed: {e:#}");
                let _ = channel_id
                    .say(&ctx.http, format!("Couldn't deliver input: {e:#}"))
                    .await;
            }
        }
    }

    async fn cleanup_sessions(
        &self,
        days: u64,
        stop_sessions: bool,
        ctx: &Context,
    ) -> Result<String> {
        anyhow::ensure!(days > 0, "cleanup age must be at least one day");
        let cutoff = unix_now().saturating_sub(days.saturating_mul(86_400));
        let old = self
            .state
            .read()
            .await
            .sessions
            .values()
            .filter(|s| !s.busy && s.last_activity > 0 && s.last_activity < cutoff)
            .cloned()
            .collect::<Vec<_>>();
        let guild = GuildId::new(self.cfg.guild_id);
        let mut channels = guild.channels(&ctx.http).await?;
        let mut archived = 0;
        for session in old {
            if let Some(channel) = channels.get_mut(&ChannelId::new(session.channel_id)) {
                let topic = format!(
                    "Archived inactive project `{}` · last activity {}",
                    session.project,
                    relative_time(session.last_activity)
                );
                if let Err(e) = channel
                    .edit(
                        &ctx.http,
                        serenity::builder::EditChannel::new().topic(topic),
                    )
                    .await
                {
                    warn!(project = %session.project, "could not archive inactive project channel: {e}");
                    continue;
                }
                if stop_sessions {
                    if let Err(e) = stop_project(&self.state, &session).await {
                        warn!(project = %session.project, "could not stop archived project session: {e}");
                    }
                }
                archived += 1;
            }
        }
        persist_state(&self.state).await?;
        Ok(format!("Archived {archived} inactive project channel(s) older than {days} days. Their project/channel links remain available; old sessions {}.", if stop_sessions { "were stopped" } else { "were left running" }))
    }

    async fn restore_sessions(&self, ctx: &Context) -> Result<()> {
        let guild = GuildId::new(self.cfg.guild_id);
        let channels = guild.channels(&ctx.http).await?;
        let panes = Command::new("tmux")
            .args(["list-panes", "-a", "-F", "#{session_name}\t#{pane_id}"])
            .output()
            .await?;
        let pane_map = String::from_utf8_lossy(&panes.stdout)
            .lines()
            .filter_map(|line| {
                let (session, pane) = line.split_once('\t')?;
                Some((session.to_owned(), pane.to_owned()))
            })
            .collect::<HashMap<_, _>>();
        let mut dirs = tokio::fs::read_dir(&self.cfg.projects_root).await?;
        let persistence_path = self.state.read().await.persistence_path.clone();
        let stored = match tokio::fs::read(&persistence_path).await {
            Ok(bytes) => serde_json::from_slice::<Vec<StoredSession>>(&bytes)
                .unwrap_or_default()
                .into_iter()
                .map(|s| (s.project.clone(), s))
                .collect::<HashMap<_, _>>(),
            Err(_) => HashMap::new(),
        };
        let mut restored = 0usize;
        while let Some(entry) = dirs.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let project = entry.file_name().to_string_lossy().into_owned();
            let tmux_name = format!("claude-{}", slug(&project));
            let channel_name = format!("claude-{}", slug(&project));
            let Some(channel) = channels
                .values()
                .find(|ch| ch.name == channel_name && ch.kind == ChannelType::Text)
            else {
                continue;
            };
            let previous = stored.get(&project);
            let claude_session_id = match previous.and_then(|s| s.claude_session_id.clone()) {
                Some(id) => Some(id),
                None => latest_claude_session_id(&self.cfg, &project).await.ok(),
            };
            self.state.write().await.sessions.insert(
                project.clone(),
                Session {
                    project: project.clone(),
                    channel_id: channel.id.get(),
                    tmux_name,
                    pane_id: pane_map
                        .get(&format!(
                            "claude-{}",
                            slug(&entry.file_name().to_string_lossy())
                        ))
                        .cloned()
                        .unwrap_or_default(),
                    transcript_path: None,
                    claude_session_id,
                    codex_thread_id: previous.and_then(|s| s.codex_thread_id.clone()),
                    active: false,
                    provider: previous
                        .map(|s| s.provider.clone())
                        .unwrap_or_else(|| "claude".into()),
                    model: previous
                        .map(|s| s.model.clone())
                        .unwrap_or_else(|| "sonnet".into()),
                    codex: None,
                    turns: previous.map(|s| s.turns).unwrap_or_default(),
                    last_activity: previous.map(|s| s.last_activity).unwrap_or_default(),
                    pending_files: Vec::new(),
                    interrupt: Arc::new(tokio::sync::Notify::new()),
                    busy: false,
                    input_tokens: previous.map(|s| s.input_tokens).unwrap_or_default(),
                    output_tokens: previous.map(|s| s.output_tokens).unwrap_or_default(),
                    cost_usd: previous.and_then(|s| s.cost_usd),
                    pinned: previous.map(|s| s.pinned).unwrap_or(false),
                    turn_queue: Arc::new(Mutex::new(())),
                    turn_done: Arc::new(tokio::sync::Notify::new()),
                },
            );
            restored += 1;
        }
        persist_state(&self.state).await?;
        info!(restored, "restored project sessions from tmux and Discord");
        Ok(())
    }
}

async fn start_project(ctx: &Context, cfg: &Config, state: &Shared, name: &str) -> Result<u64> {
    anyhow::ensure!(
        name.chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_".contains(c)),
        "project name may contain only letters, numbers, `-`, and `_`"
    );
    let root = tokio::fs::canonicalize(&cfg.projects_root)
        .await
        .context("PROJECTS_ROOT does not exist")?;
    let cwd = tokio::fs::canonicalize(root.join(name))
        .await
        .context("project directory not found directly under PROJECTS_ROOT")?;
    anyhow::ensure!(
        cwd.starts_with(&root) && cwd.is_dir(),
        "project must be a directory directly under PROJECTS_ROOT"
    );
    if let Some(s) = state.read().await.sessions.get(name).cloned() {
        if pane_exists(&s.tmux_name).await {
            return Ok(s.channel_id);
        }
    }

    let guild = GuildId::new(cfg.guild_id);
    let channel_id = if let Some(s) = state.read().await.sessions.get(name) {
        s.channel_id
    } else {
        let existing = guild.channels(&ctx.http).await?;
        let expected = format!("claude-{}", slug(name));
        if let Some(ch) = existing
            .values()
            .find(|ch| ch.name == expected && ch.kind == ChannelType::Text)
        {
            ch.id.get()
        } else {
            let mut builder = serenity::builder::CreateChannel::new(expected).topic(format!(
                "Project `{name}` · codebase `{}` · Claude/Codex session",
                cwd.display()
            ));
            if let Some(cat) = cfg.category_id {
                builder = builder.category(serenity::model::id::ChannelId::new(cat));
            }
            guild.create_channel(&ctx.http, builder).await?.id.get()
        }
    };

    let tmux_name = format!("claude-{}", slug(name));
    if pane_exists(&tmux_name).await {
        let _ = Command::new("tmux")
            .args(["kill-session", "-t", &tmux_name])
            .status()
            .await;
    }
    let pane = Command::new("tmux")
        .args([
            "new-session",
            "-d",
            "-P",
            "-F",
            "#{pane_id}",
            "-s",
            &tmux_name,
            "-c",
            cwd.to_str()
                .ok_or_else(|| anyhow!("project path is not UTF-8"))?,
            "-e",
            &format!("CLAUDE_DISCORD_HOOK_URL={}", cfg.hook_url),
            "-e",
            &format!("CLAUDE_DISCORD_HOOK_SECRET={}", cfg.secret),
        ])
        .output()
        .await
        .context("tmux is required and must be on PATH")?;
    anyhow::ensure!(
        pane.status.success(),
        "tmux could not create session: {}",
        String::from_utf8_lossy(&pane.stderr)
    );
    let pane_id = String::from_utf8(pane.stdout)?.trim().to_string();
    state.write().await.sessions.insert(
        name.to_string(),
        Session {
            project: name.into(),
            channel_id,
            tmux_name: tmux_name.clone(),
            pane_id: pane_id.clone(),
            transcript_path: None,
            claude_session_id: None,
            codex_thread_id: None,
            active: true,
            provider: "claude".into(),
            model: "sonnet".into(),
            codex: None,
            turns: 0,
            last_activity: unix_now(),
            pending_files: Vec::new(),
            interrupt: Arc::new(tokio::sync::Notify::new()),
            busy: false,
            input_tokens: 0,
            output_tokens: 0,
            cost_usd: None,
            pinned: false,
            turn_queue: Arc::new(Mutex::new(())),
            turn_done: Arc::new(tokio::sync::Notify::new()),
        },
    );
    persist_state(state).await?;

    let settings = make_settings(cfg)?;
    let claude = env::var("CLAUDE_COMMAND").unwrap_or_else(|_| "claude".into());
    let command = format!(
        "{} --settings {} --model sonnet",
        shell_quote(&claude),
        shell_quote(&settings.to_string_lossy())
    );
    let mut p = Command::new("tmux")
        .args(["load-buffer", "-b", "claude-bridge", "-"])
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    p.stdin
        .take()
        .context("tmux stdin unavailable")?
        .write_all(command.as_bytes())
        .await?;
    anyhow::ensure!(
        p.wait().await?.success(),
        "could not pass Claude command to tmux"
    );
    let paste = Command::new("tmux")
        .args(["paste-buffer", "-b", "claude-bridge", "-p", "-t", &pane_id])
        .status()
        .await?;
    anyhow::ensure!(paste.success(), "could not paste Claude command into tmux");
    anyhow::ensure!(
        Command::new("tmux")
            .args(["send-keys", "-t", &pane_id, "Enter"])
            .status()
            .await?
            .success(),
        "could not start Claude Code"
    );
    let _ = ChannelId::new(channel_id)
        .say(
            &ctx.http,
            format!("Starting Claude Code in `{}`.", cwd.display()),
        )
        .await;
    Ok(channel_id)
}

async fn connect_project(
    ctx: &Context,
    cfg: &Config,
    state: &Shared,
    name: &str,
    provider: &str,
) -> Result<u64> {
    anyhow::ensure!(
        name.chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_".contains(c)),
        "project name may contain only letters, numbers, `-`, and `_`"
    );
    anyhow::ensure!(
        provider == "claude" || provider == "codex",
        "provider must be `claude` or `codex`"
    );
    let root = tokio::fs::canonicalize(&cfg.projects_root).await?;
    let cwd = tokio::fs::canonicalize(root.join(name)).await?;
    anyhow::ensure!(
        cwd.starts_with(&root) && cwd.is_dir(),
        "project must be a directory under PROJECTS_ROOT"
    );
    if let Some(channel_id) = state.read().await.sessions.get(name).map(|s| s.channel_id) {
        let mut guard = state.write().await;
        if let Some(session) = guard.sessions.get_mut(name) {
            if session.provider != provider {
                session.provider = provider.to_owned();
                session.model = if provider == "codex" {
                    "default"
                } else {
                    "sonnet"
                }
                .to_owned();
            }
            session.codex = None;
            session.active = false;
        }
        drop(guard);
        persist_state(state).await?;
        return Ok(channel_id);
    }
    let guild = GuildId::new(cfg.guild_id);
    let channels = guild.channels(&ctx.http).await?;
    let expected = format!("claude-{}", slug(name));
    let channel = channels
        .values()
        .find(|ch| ch.name == expected && ch.kind == ChannelType::Text)
        .context("project channel does not exist yet; use `start <project>` once to create it")?;
    state.write().await.sessions.insert(
        name.to_owned(),
        Session {
            project: name.to_owned(),
            channel_id: channel.id.get(),
            tmux_name: String::new(),
            pane_id: String::new(),
            transcript_path: None,
            claude_session_id: None,
            codex_thread_id: None,
            active: false,
            provider: provider.to_owned(),
            model: if provider == "codex" {
                "default"
            } else {
                "sonnet"
            }
            .to_owned(),
            codex: None,
            turns: 0,
            last_activity: unix_now(),
            pending_files: Vec::new(),
            interrupt: Arc::new(tokio::sync::Notify::new()),
            busy: false,
            input_tokens: 0,
            output_tokens: 0,
            cost_usd: None,
            pinned: false,
            turn_queue: Arc::new(Mutex::new(())),
            turn_done: Arc::new(tokio::sync::Notify::new()),
        },
    );
    persist_state(state).await?;
    Ok(channel.id.get())
}

fn make_settings(cfg: &Config) -> Result<PathBuf> {
    let path = env::temp_dir().join("claude-discord-bridge");
    std::fs::create_dir_all(&path)?;
    let exe = env::current_exe()?;
    let command = format!("{} hook", shell_quote(&exe.to_string_lossy()));
    let hook =
        |timeout| serde_json::json!({"type":"command", "command":command, "timeout":timeout});
    let settings = serde_json::json!({"hooks": {
        "SessionStart": [{"hooks":[hook(10)]}],
        "UserPromptSubmit": [{"hooks":[hook(10)]}],
        "Stop": [{"hooks":[hook(30)]}],
        "SessionEnd": [{"hooks":[hook(10)]}]
    }});
    let file = path.join("hooks.json");
    std::fs::write(&file, serde_json::to_vec_pretty(&settings)?)?;
    let _ = cfg;
    Ok(file)
}

async fn pane_exists(name: &str) -> bool {
    Command::new("tmux")
        .args(["has-session", "-t", name])
        .status()
        .await
        .map(|x| x.success())
        .unwrap_or(false)
}

async fn send_to_claude(s: &Session, text: &str) -> Result<()> {
    anyhow::ensure!(
        s.active,
        "Claude session has ended; use `!resume` to continue the conversation"
    );
    anyhow::ensure!(
        pane_exists(&s.tmux_name).await,
        "session is no longer running; DM `start {}` to restart it",
        s.project
    );
    let mut p = Command::new("tmux")
        .args(["load-buffer", "-b", "claude-bridge-in", "-"])
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    p.stdin
        .take()
        .context("tmux stdin unavailable")?
        .write_all(text.as_bytes())
        .await?;
    anyhow::ensure!(p.wait().await?.success(), "tmux load-buffer failed");
    anyhow::ensure!(
        Command::new("tmux")
            .args([
                "paste-buffer",
                "-b",
                "claude-bridge-in",
                "-p",
                "-t",
                &s.pane_id
            ])
            .status()
            .await?
            .success(),
        "tmux paste failed"
    );
    anyhow::ensure!(
        Command::new("tmux")
            .args(["send-keys", "-t", &s.pane_id, "Enter"])
            .status()
            .await?
            .success(),
        "tmux send-keys failed"
    );
    Ok(())
}

async fn resume_session(cfg: &Config, state: &Shared, s: &Session) -> Result<()> {
    if s.provider == "codex" {
        anyhow::ensure!(!s.active, "Codex session is already connected");
        let root = tokio::fs::canonicalize(&cfg.projects_root).await?;
        let cwd = tokio::fs::canonicalize(root.join(&s.project)).await?;
        let thread_id = match s.codex_thread_id.clone() {
            Some(id) => id,
            None => latest_codex_thread_id(&cwd).await?,
        };
        let runtime = CodexRuntime::attach(&cwd, &s.model, &thread_id).await?;
        let saved_id = runtime.thread_id.clone();
        let mut guard = state.write().await;
        let current = guard
            .sessions
            .get_mut(&s.project)
            .context("session disappeared while resuming")?;
        current.codex = Some(Arc::new(Mutex::new(runtime)));
        current.codex_thread_id = Some(saved_id);
        current.active = true;
        drop(guard);
        persist_state(state).await?;
        return Ok(());
    }
    anyhow::ensure!(s.provider == "claude", "Codex sessions stay attached to their app-server thread; use `!status` to check availability");
    anyhow::ensure!(!s.active, "Claude session is already active");
    let discovered_id;
    let session_id = if let Some(id) = s.claude_session_id.as_deref() {
        id
    } else {
        discovered_id = latest_claude_session_id(cfg, &s.project).await?;
        &discovered_id
    };
    let root = tokio::fs::canonicalize(&cfg.projects_root).await?;
    let cwd = tokio::fs::canonicalize(root.join(&s.project)).await?;
    anyhow::ensure!(
        cwd.starts_with(&root) && cwd.is_dir(),
        "project codebase is unavailable under PROJECTS_ROOT"
    );
    let (tmux_name, pane_id) =
        launch_claude(cfg, &s.project, &cwd, &s.model, Some(session_id)).await?;
    let mut guard = state.write().await;
    let current = guard
        .sessions
        .get_mut(&s.project)
        .context("session disappeared while resuming")?;
    current.tmux_name = tmux_name;
    current.pane_id = pane_id;
    current.claude_session_id = Some(session_id.to_owned());
    current.active = false;
    drop(guard);
    persist_state(state).await?;
    Ok(())
}

async fn latest_codex_thread_id(cwd: &Path) -> Result<String> {
    let mut runtime = CodexRuntime::connect(cwd).await?;
    let response = runtime
        .rpc(
            "thread/list",
            serde_json::json!({"limit":1,"sortKey":"updated_at","cwd":cwd}),
        )
        .await?;
    response
        .pointer("/result/data/0/id")
        .and_then(|x| x.as_str())
        .map(str::to_owned)
        .context("no Codex conversation found for this codebase")
}

async fn latest_claude_session_id(cfg: &Config, project: &str) -> Result<String> {
    let root = tokio::fs::canonicalize(&cfg.projects_root).await?;
    let cwd = tokio::fs::canonicalize(root.join(project)).await?;
    let encoded = cwd.to_string_lossy().replace('/', "-");
    let dir = PathBuf::from(env::var("HOME")?)
        .join(".claude/projects")
        .join(encoded);
    let mut entries = tokio::fs::read_dir(&dir)
        .await
        .context("Claude history not found for this codebase")?;
    let mut newest: Option<(std::time::SystemTime, String)> = None;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.path().extension().is_some_and(|x| x == "jsonl") {
            continue;
        }
        let Some(id) = entry
            .path()
            .file_stem()
            .and_then(|x| x.to_str())
            .map(str::to_owned)
        else {
            continue;
        };
        let modified = entry
            .metadata()
            .await?
            .modified()
            .unwrap_or(std::time::UNIX_EPOCH);
        if newest.as_ref().is_none_or(|(time, _)| modified > *time) {
            newest = Some((modified, id));
        }
    }
    newest
        .map(|(_, id)| id)
        .context("no Claude conversations found for this codebase")
}

async fn list_claude_sessions(cfg: &Config, project: &str) -> Result<String> {
    let root = tokio::fs::canonicalize(&cfg.projects_root).await?;
    let cwd = tokio::fs::canonicalize(root.join(project)).await?;
    let encoded = cwd.to_string_lossy().replace('/', "-");
    let dir = PathBuf::from(env::var("HOME")?)
        .join(".claude/projects")
        .join(encoded);
    let mut entries = tokio::fs::read_dir(&dir)
        .await
        .context("Claude has no local history folder for this codebase")?;
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.path().extension().is_some_and(|x| x == "jsonl") {
            let modified = entry.metadata().await?.modified().ok();
            files.push((modified, entry.path()));
        }
    }
    files.sort_by(|a, b| b.0.cmp(&a.0));
    if files.is_empty() {
        return Ok("No Claude conversations found for this codebase.".into());
    }
    let mut out = format!("Claude conversations for `{}` (newest first):\n", project);
    for (_, path) in files.into_iter().take(10) {
        if let Some(id) = path.file_stem().and_then(|x| x.to_str()) {
            out.push_str(&format!(
                "`{id}` · {}\n",
                path.file_name().unwrap_or_default().to_string_lossy()
            ));
        }
    }
    out.push_str("Attach with `!attach <id>`. Only conversations in this project's Claude history folder are listed.");
    Ok(out)
}

async fn list_codex_sessions(cfg: &Config, s: &Session) -> Result<String> {
    let root = tokio::fs::canonicalize(&cfg.projects_root).await?;
    let cwd = tokio::fs::canonicalize(root.join(&s.project)).await?;
    let mut runtime = CodexRuntime::connect(&cwd).await?;
    let response = runtime
        .rpc(
            "thread/list",
            serde_json::json!({"limit":50,"sortKey":"updated_at","cwd":cwd}),
        )
        .await?;
    let threads = response
        .pointer("/result/data")
        .and_then(|x| x.as_array())
        .context("Codex thread list missing")?;
    if threads.is_empty() {
        return Ok("No Codex conversations found for this codebase.".into());
    }
    let mut out = format!("Codex conversations for `{}` (newest first):\n", s.project);
    for t in threads.iter().take(10) {
        let id = t.get("id").and_then(|x| x.as_str()).unwrap_or("unknown");
        let preview = t
            .get("name")
            .and_then(|x| x.as_str())
            .or_else(|| t.get("preview").and_then(|x| x.as_str()))
            .unwrap_or("(no title)");
        let preview: String = preview.chars().take(100).collect();
        out.push_str(&format!("`{id}` · {preview}\n"));
    }
    out.push_str(
        "Attach with `!attach <thread-id>`. Results are filtered to this exact codebase path.",
    );
    Ok(out)
}

async fn attach_existing(cfg: &Config, state: &Shared, s: &Session, id: &str) -> Result<()> {
    anyhow::ensure!(
        id.len() <= 100
            && id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_".contains(c)),
        "conversation ID contains invalid characters"
    );
    let root = tokio::fs::canonicalize(&cfg.projects_root).await?;
    let cwd = tokio::fs::canonicalize(root.join(&s.project)).await?;
    anyhow::ensure!(
        cwd.starts_with(&root) && cwd.is_dir(),
        "project codebase is unavailable"
    );
    if s.provider == "claude" {
        let (tmux_name, pane_id) = launch_claude(cfg, &s.project, &cwd, &s.model, Some(id)).await?;
        let mut guard = state.write().await;
        let current = guard
            .sessions
            .get_mut(&s.project)
            .context("session disappeared while attaching")?;
        current.tmux_name = tmux_name;
        current.pane_id = pane_id;
        current.claude_session_id = Some(id.to_owned());
        current.active = false;
        current.transcript_path = None;
        drop(guard);
        persist_state(state).await?;
    } else {
        let runtime = CodexRuntime::attach(&cwd, &s.model, id).await?;
        let saved_id = runtime.thread_id.clone();
        let mut guard = state.write().await;
        let current = guard
            .sessions
            .get_mut(&s.project)
            .context("session disappeared while attaching")?;
        current.codex = Some(Arc::new(Mutex::new(runtime)));
        current.codex_thread_id = Some(saved_id);
        current.active = true;
        drop(guard);
        persist_state(state).await?;
    }
    Ok(())
}

async fn switch_provider(
    cfg: &Config,
    state: &Shared,
    s: &Session,
    provider: &str,
    model: &str,
) -> Result<()> {
    anyhow::ensure!(
        provider == "claude" || provider == "codex",
        "provider must be `claude` or `codex`"
    );
    let root = tokio::fs::canonicalize(&cfg.projects_root).await?;
    let cwd = tokio::fs::canonicalize(root.join(&s.project)).await?;
    if provider == s.provider {
        return change_model(state, s, model).await;
    }
    let (tmux_name, pane_id, codex) = if provider == "codex" {
        let codex = CodexRuntime::launch(&cwd, model).await?;
        if pane_exists(&s.tmux_name).await {
            let _ = Command::new("tmux")
                .args(["kill-session", "-t", &s.tmux_name])
                .status()
                .await;
        }
        (
            String::new(),
            String::new(),
            Some(Arc::new(Mutex::new(codex))),
        )
    } else {
        let (name, pane) = launch_claude(cfg, &s.project, &cwd, model, None).await?;
        (name, pane, None)
    };
    let codex_thread_id = if let Some(runtime) = &codex {
        Some(runtime.lock().await.thread_id.clone())
    } else {
        None
    };
    let mut guard = state.write().await;
    let current = guard
        .sessions
        .get_mut(&s.project)
        .context("session disappeared while switching providers")?;
    current.provider = provider.to_owned();
    current.model = model.to_owned();
    current.tmux_name = tmux_name;
    current.pane_id = pane_id;
    current.transcript_path = None;
    current.active = provider == "claude";
    if let Some(thread_id) = codex_thread_id {
        current.codex_thread_id = Some(thread_id);
    }
    current.codex = codex;
    drop(guard);
    persist_state(state).await?;
    Ok(())
}

async fn change_model(state_shared: &Shared, s: &Session, model: &str) -> Result<()> {
    anyhow::ensure!(
        !model.is_empty()
            && model.len() <= 100
            && model
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c)),
        "model name must contain only letters, numbers, `.`, `_`, or `-`"
    );
    if s.provider == "claude" {
        anyhow::ensure!(
            pane_exists(&s.tmux_name).await,
            "Claude session is not running"
        );
        let command = format!("/model {model}");
        let mut p = Command::new("tmux")
            .args(["load-buffer", "-b", "claude-bridge-model", "-"])
            .stdin(std::process::Stdio::piped())
            .spawn()?;
        p.stdin
            .take()
            .context("tmux stdin unavailable")?
            .write_all(command.as_bytes())
            .await?;
        anyhow::ensure!(p.wait().await?.success(), "tmux load-buffer failed");
        anyhow::ensure!(
            Command::new("tmux")
                .args([
                    "paste-buffer",
                    "-b",
                    "claude-bridge-model",
                    "-p",
                    "-t",
                    &s.pane_id
                ])
                .status()
                .await?
                .success(),
            "could not send model command"
        );
        anyhow::ensure!(
            Command::new("tmux")
                .args(["send-keys", "-t", &s.pane_id, "Enter"])
                .status()
                .await?
                .success(),
            "could not submit model command"
        );
    }
    let mut guard = state_shared.write().await;
    let current = guard
        .sessions
        .get_mut(&s.project)
        .context("session disappeared")?;
    current.model = model.to_owned();
    drop(guard);
    persist_state(state_shared).await?;
    Ok(())
}

async fn launch_claude(
    cfg: &Config,
    project: &str,
    cwd: &Path,
    model: &str,
    resume_id: Option<&str>,
) -> Result<(String, String)> {
    let tmux_name = format!("claude-{}", slug(project));
    if pane_exists(&tmux_name).await {
        let _ = Command::new("tmux")
            .args(["kill-session", "-t", &tmux_name])
            .status()
            .await;
    }
    let pane = Command::new("tmux")
        .args([
            "new-session",
            "-d",
            "-P",
            "-F",
            "#{pane_id}",
            "-s",
            &tmux_name,
            "-c",
            cwd.to_str().context("project path is not UTF-8")?,
            "-e",
            &format!("CLAUDE_DISCORD_HOOK_URL={}", cfg.hook_url),
            "-e",
            &format!("CLAUDE_DISCORD_HOOK_SECRET={}", cfg.secret),
        ])
        .output()
        .await
        .context("tmux is required and must be on PATH")?;
    anyhow::ensure!(
        pane.status.success(),
        "tmux could not create session: {}",
        String::from_utf8_lossy(&pane.stderr)
    );
    let pane_id = String::from_utf8(pane.stdout)?.trim().to_owned();
    let settings = make_settings(cfg)?;
    let claude = env::var("CLAUDE_COMMAND").unwrap_or_else(|_| "claude".into());
    let command = format!(
        "{} --settings {} --model {}{}",
        shell_quote(&claude),
        shell_quote(&settings.to_string_lossy()),
        shell_quote(model),
        resume_id
            .map(|id| format!(" --resume {}", shell_quote(id)))
            .unwrap_or_default()
    );
    let mut input = Command::new("tmux")
        .args(["load-buffer", "-b", "claude-bridge-start", "-"])
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    input
        .stdin
        .take()
        .context("tmux stdin unavailable")?
        .write_all(command.as_bytes())
        .await?;
    anyhow::ensure!(input.wait().await?.success(), "tmux load-buffer failed");
    anyhow::ensure!(
        Command::new("tmux")
            .args([
                "paste-buffer",
                "-b",
                "claude-bridge-start",
                "-p",
                "-t",
                &pane_id
            ])
            .status()
            .await?
            .success(),
        "could not start Claude Code"
    );
    anyhow::ensure!(
        Command::new("tmux")
            .args(["send-keys", "-t", &pane_id, "Enter"])
            .status()
            .await?
            .success(),
        "could not submit Claude command"
    );
    Ok((tmux_name, pane_id))
}

impl CodexRuntime {
    async fn launch(cwd: &Path, model: &str) -> Result<Self> {
        let mut runtime = Self::connect(cwd).await?;
        let mut params =
            serde_json::json!({"cwd":cwd,"approvalPolicy":"never","sandbox":"workspace-write"});
        if model != "default" {
            params["model"] = serde_json::Value::String(model.to_owned());
        }
        let start = runtime.rpc("thread/start", params).await?;
        runtime.thread_id = start
            .pointer("/result/thread/id")
            .and_then(|x| x.as_str())
            .context("Codex did not return a thread ID")?
            .to_owned();
        Ok(runtime)
    }

    async fn attach(cwd: &Path, model: &str, thread_id: &str) -> Result<Self> {
        let mut runtime = Self::connect(cwd).await?;
        let response = runtime.rpc("thread/resume", serde_json::json!({"threadId":thread_id,"cwd":cwd,"model":model,"approvalPolicy":"never","sandbox":"workspace-write","excludeTurns":true})).await?;
        runtime.thread_id = response
            .pointer("/result/thread/id")
            .and_then(|x| x.as_str())
            .unwrap_or(thread_id)
            .to_owned();
        Ok(runtime)
    }

    async fn connect(cwd: &Path) -> Result<Self> {
        let mut child = Command::new(env::var("CODEX_COMMAND").unwrap_or_else(|_| "codex".into()))
            .args(["app-server", "--listen", "stdio://"])
            .current_dir(cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .context("could not start Codex app-server; install and sign in to Codex CLI")?;
        let stdin = child.stdin.take().context("Codex stdin unavailable")?;
        let stdout = child.stdout.take().context("Codex stdout unavailable")?;
        let mut runtime = Self {
            _child: child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            thread_id: String::new(),
            next_id: 1,
        };
        let init = runtime.rpc("initialize", serde_json::json!({"clientInfo":{"name":"claude-discord-bridge","title":"Claude Discord Bridge","version":"0.1.0"}})).await?;
        anyhow::ensure!(
            init.get("result").is_some(),
            "Codex app-server initialization failed: {init}"
        );
        runtime.notify("initialized", serde_json::json!({})).await?;
        Ok(runtime)
    }

    async fn notify(&mut self, method: &str, params: serde_json::Value) -> Result<()> {
        let mut value = serde_json::json!({"method":method,"params":params});
        value["jsonrpc"] = serde_json::Value::String("2.0".into());
        self.stdin
            .write_all(format!("{}\n", value).as_bytes())
            .await?;
        self.stdin.flush().await?;
        Ok(())
    }

    async fn rpc(&mut self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let id = self.next_id;
        self.next_id += 1;
        let mut value = serde_json::json!({"method":method,"id":id,"params":params});
        value["jsonrpc"] = serde_json::Value::String("2.0".into());
        self.stdin
            .write_all(format!("{}\n", value).as_bytes())
            .await?;
        self.stdin.flush().await?;
        loop {
            let line = self
                .stdout
                .next_line()
                .await?
                .context("Codex app-server closed stdout")?;
            let response: serde_json::Value =
                serde_json::from_str(&line).context("invalid JSON from Codex app-server")?;
            if response.get("id").and_then(|x| x.as_u64()) == Some(id) {
                if let Some(error) = response.get("error") {
                    return Err(anyhow!("Codex RPC failed: {error}"));
                }
                return Ok(response);
            }
        }
    }

    async fn models(&mut self) -> Result<Vec<String>> {
        let response = self
            .rpc("model/list", serde_json::json!({"limit":100}))
            .await?;
        Ok(response
            .pointer("/result/data")
            .and_then(|x| x.as_array())
            .context("Codex model list missing")?
            .iter()
            .filter_map(|m| m.get("id").and_then(|x| x.as_str()).map(str::to_owned))
            .collect())
    }

    async fn turn(
        &mut self,
        prompt: &str,
        model: &str,
        interrupt: &tokio::sync::Notify,
    ) -> Result<(String, u64, u64)> {
        let id = self.next_id;
        self.next_id += 1;
        let mut params = serde_json::json!({"threadId":self.thread_id,"input":[{"type":"text","text":prompt}],"approvalPolicy":"never","sandboxPolicy":{"type":"workspaceWrite"}});
        if model != "default" {
            params["model"] = serde_json::Value::String(model.to_owned());
        }
        let request =
            serde_json::json!({"jsonrpc":"2.0","method":"turn/start","id":id,"params":params});
        self.stdin
            .write_all(format!("{}\n", request).as_bytes())
            .await?;
        self.stdin.flush().await?;
        let mut output = String::new();
        let mut acknowledged = false;
        let mut turn_id: Option<String> = None;
        let mut input_tokens = 0;
        let mut output_tokens = 0;
        let mut interrupt_requested = false;
        loop {
            let line = tokio::select! {
                result = tokio::time::timeout(std::time::Duration::from_secs(900), self.stdout.next_line()) => Some(result??.context("Codex app-server closed stdout during turn")?),
                _ = interrupt.notified() => {
                    interrupt_requested = true;
                    None
                }
            };
            let Some(line) = line else {
                if let Some(turn_id) = turn_id.as_deref() {
                    let _ = self
                        .rpc(
                            "turn/interrupt",
                            serde_json::json!({"threadId": self.thread_id, "turnId": turn_id}),
                        )
                        .await;
                    return Err(anyhow!("Codex turn interrupted"));
                }
                continue;
            };
            let event: serde_json::Value =
                serde_json::from_str(&line).context("invalid JSON from Codex app-server")?;
            if event.get("id").and_then(|x| x.as_u64()) == Some(id) {
                if let Some(error) = event.get("error") {
                    return Err(anyhow!("Codex turn failed to start: {error}"));
                }
                acknowledged = true;
                turn_id = event
                    .pointer("/result/turn/id")
                    .and_then(|x| x.as_str())
                    .map(str::to_owned);
            }
            match event.get("method").and_then(|x| x.as_str()) {
                Some("thread/tokenUsage/updated")
                    if event.pointer("/params/threadId").and_then(|x| x.as_str())
                        == Some(&self.thread_id) =>
                {
                    if let Some(total) = event.pointer("/params/tokenUsage/total") {
                        input_tokens = total
                            .get("inputTokens")
                            .and_then(|x| x.as_u64())
                            .unwrap_or(input_tokens);
                        output_tokens = total
                            .get("outputTokens")
                            .and_then(|x| x.as_u64())
                            .unwrap_or(output_tokens);
                    }
                }
                Some("turn/started")
                    if event.pointer("/params/threadId").and_then(|x| x.as_str())
                        == Some(&self.thread_id) =>
                {
                    turn_id = event
                        .pointer("/params/turn/id")
                        .and_then(|x| x.as_str())
                        .map(str::to_owned)
                        .or(turn_id);
                }
                Some("item/agentMessage/delta") => {
                    if let Some(delta) = event.pointer("/params/delta").and_then(|x| x.as_str()) {
                        output.push_str(delta);
                    }
                }
                Some("turn/completed")
                    if event.pointer("/params/threadId").and_then(|x| x.as_str())
                        == Some(&self.thread_id) =>
                {
                    let status = event
                        .pointer("/params/turn/status")
                        .and_then(|x| x.as_str())
                        .unwrap_or("unknown");
                    anyhow::ensure!(
                        status == "completed",
                        "Codex turn ended with status `{status}`"
                    );
                    break;
                }
                Some("error") => {
                    return Err(anyhow!(
                        "Codex app-server error: {}",
                        event.get("params").unwrap_or(&event)
                    ))
                }
                _ => {}
            }
            if interrupt_requested {
                if let Some(turn_id) = turn_id.as_deref() {
                    let _ = self
                        .rpc(
                            "turn/interrupt",
                            serde_json::json!({"threadId": self.thread_id, "turnId": turn_id}),
                        )
                        .await;
                    return Err(anyhow!("Codex turn interrupted"));
                }
            }
        }
        let _ = acknowledged;
        anyhow::ensure!(
            !output.trim().is_empty(),
            "Codex completed without a text reply"
        );
        Ok((output, input_tokens, output_tokens))
    }
}

async fn receive_hook(
    State((state, cfg)): State<(Shared, Arc<Config>)>,
    headers: HeaderMap,
    Json(event): Json<HookEvent>,
) -> StatusCode {
    if headers
        .get("x-claude-bridge-secret")
        .and_then(|x| x.to_str().ok())
        != Some(cfg.secret.as_str())
    {
        return StatusCode::UNAUTHORIZED;
    }
    let kind = event.hook_event_name.as_deref().unwrap_or("<missing>");
    let pane = event.tmux_pane.clone();
    let Some(pane) = pane else {
        warn!(event = kind, "Claude hook arrived without a tmux pane ID");
        return StatusCode::NO_CONTENT;
    };
    let mut guard = state.write().await;
    let Some(s) = guard.sessions.values_mut().find(|s| s.pane_id == pane) else {
        warn!(event = kind, pane, "Claude hook pane is not registered");
        return StatusCode::NO_CONTENT;
    };
    if let Some(p) = event.transcript_path {
        s.transcript_path = Some(p);
    }
    info!(event = kind, pane, project = %s.project, "received Claude hook");
    if kind == "SessionStart" {
        if let Some(id) = event.session_id.clone() {
            s.claude_session_id = Some(id);
        }
        s.active = true;
        s.busy = false;
    } else if kind == "SessionEnd" {
        if let Some(id) = event.session_id.clone() {
            s.claude_session_id = Some(id);
        }
        s.active = false;
        s.busy = false;
    } else if kind == "UserPromptSubmit" {
        s.busy = true;
        s.last_activity = unix_now();
        s.turns = s.turns.saturating_add(1);
    } else if kind == "Stop" {
        s.busy = false;
        let (input, output, cost) = transcript_usage(s.transcript_path.as_deref());
        s.input_tokens = input;
        s.output_tokens = output;
        s.cost_usd = cost;
    }
    let text = match kind {
        "SessionStart" => Some(format!("Claude session started for `{}`.", s.project)),
        "UserPromptSubmit" => Some(format!("Processing your message in `{}`…", s.project)),
        "SessionEnd" => Some(format!("Claude left the active session for `{}`. Send `!resume` here to continue this conversation in `{}`.", s.project, s.project)),
        "Stop" => latest_assistant_message(s.transcript_path.as_deref()).map(|answer| format!("Task finished.\n\n{answer}")),
        _ => None,
    };
    let pending_files = if kind == "Stop" {
        std::mem::take(&mut s.pending_files)
    } else {
        Vec::new()
    };
    let turn_done = s.turn_done.clone();
    if kind == "Stop" && text.is_none() {
        warn!(project = %s.project, transcript = ?s.transcript_path, "Stop hook had no assistant text to post");
    }
    let channel = s.channel_id;
    drop(guard);
    if kind == "Stop" || kind == "SessionEnd" {
        turn_done.notify_one();
    }
    remove_temp_files(pending_files).await;
    if let Err(e) = persist_state(&state).await {
        error!("could not persist session state: {e:#}");
    }
    if let Some(text) = text {
        for part in discord_chunks(&text) {
            if let Err(e) = ChannelId::new(channel)
                .say(DISCORD_HTTP.get().unwrap(), part)
                .await
            {
                error!("Discord post failed: {e}");
            }
        }
    }
    StatusCode::NO_CONTENT
}

fn latest_assistant_message(path: Option<&Path>) -> Option<String> {
    let content = std::fs::read_to_string(path?).ok()?;
    let mut latest = None;
    for line in content.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if v.get("type").and_then(|x| x.as_str()) != Some("assistant") {
            continue;
        }
        let mut out = String::new();
        if let Some(items) = v.pointer("/message/content").and_then(|x| x.as_array()) {
            for item in items {
                if item.get("type").and_then(|x| x.as_str()) == Some("text") {
                    if let Some(t) = item.get("text").and_then(|x| x.as_str()) {
                        if !out.is_empty() {
                            out.push('\n');
                        }
                        out.push_str(t);
                    }
                }
            }
        }
        if !out.trim().is_empty() {
            latest = Some(out);
        }
    }
    latest
}

async fn hook_main() -> Result<()> {
    let mut input = String::new();
    tokio::io::stdin().read_to_string(&mut input).await?;
    let mut body: HookEvent = serde_json::from_str(&input)?;
    body.tmux_pane = env::var("TMUX_PANE").ok();
    let url = env::var("CLAUDE_DISCORD_HOOK_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8787/hooks/claude".into());
    let secret = env::var("CLAUDE_DISCORD_HOOK_SECRET").context("missing hook secret")?;
    let client = reqwest::Client::new();
    let response = client
        .post(url)
        .header("x-claude-bridge-secret", secret)
        .json(&body)
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "bridge returned {}",
        response.status()
    );
    Ok(())
}

fn slug(s: &str) -> String {
    let v = s
        .to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();
    v.trim_matches('-').chars().take(80).collect()
}
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}
fn discord_chunks(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    for line in s.lines() {
        if buf.len() + line.len() + 1 > 1900 && !buf.is_empty() {
            out.push(std::mem::take(&mut buf));
        }
        if !buf.is_empty() {
            buf.push('\n');
        }
        buf.push_str(line);
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    out
}
