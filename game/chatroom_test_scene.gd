extends Control

@onready var bridge: ChatroomBridge = $ChatroomBridge
@onready var stage_status_panel: StageStatusPanel = $StageStatusPanel
@onready var status_label: Label = $StatusLabel
@onready var stage_snapshot_label: Label = $StageSnapshotLabel
@onready var messages_scroll: ScrollContainer = $MessagesScroll
@onready var messages_list: VBoxContainer = $MessagesScroll/MessagesList
@onready var composer_input: LineEdit = $Composer/ComposerInput
@onready var send_button: Button = $Composer/SendButton

const MAX_LINES: int = 200
const FAKE_REPLY_DELAY_SEC: float = 0.75
const MESSAGE_BUBBLE_SCENE: PackedScene = preload("res://message_bubble.tscn")
const SAVE_FILE_PATH: String = "user://conversation_history.json"
const STATUS_IDLE: String = "Status: idle"

# Keeping speaker constants avoids typo-driven state corruption.
# Conversation data is routed through one append path on purpose.
# This gives us a tiny but durable model boundary for Task 4/9.
const SPEAKER_USER: String = "User"
const SPEAKER_PIPER: String = "Piper"
const SPEAKER_MASON: String = "Mason"
const SPEAKER_VERA: String = "Vera"
const SPEAKER_TESS: String = "Tess"
const SPEAKER_SYSTEM: String = "System"
const AGENT_SPEAKERS: PackedStringArray = [SPEAKER_PIPER, SPEAKER_MASON, SPEAKER_VERA, SPEAKER_TESS]

var _conversation: Array[Dictionary] = []
var _agent_turn_index: int = 0

func _ready() -> void:
    _set_status(STATUS_IDLE)

    bridge.broker_connected.connect(_on_broker_connected)
    bridge.broker_disconnected.connect(_on_broker_disconnected)
    bridge.broker_error.connect(_on_broker_error)
    bridge.chat_event_received.connect(_on_chat_event_received)
    bridge.stage_status_changed.connect(_on_stage_status_changed)
    stage_status_panel.status_snapshot_updated.connect(_on_status_snapshot_updated)

    # Button and Enter-key share one send path.
    # We disable send until there is valid input to reduce accidental sends.
    # This keeps the typing loop predictable for repeated test runs.
    send_button.pressed.connect(_on_send_pressed)
    composer_input.text_submitted.connect(_on_input_submitted)
    composer_input.text_changed.connect(_on_input_text_changed)
    _sync_send_button_state()
    composer_input.grab_focus()

    _load_conversation_from_disk()

    _append_system_line("Chatroom test scene ready. Waiting for events...")
    stage_snapshot_label.text = "Stage snapshot: waiting for stage_status.json"


func _on_broker_connected() -> void:
    _set_status("Status: connected")
    _append_system_line("Broker connected")
    print("[ChatroomTest] Broker connected")


func _on_broker_disconnected() -> void:
    _set_status("Status: reconnecting...")
    _append_system_line("Broker disconnected")
    print("[ChatroomTest] Broker disconnected")


func _on_broker_error(message: String) -> void:
    _append_system_line("Broker error: %s" % message)
    print("[ChatroomTest] Broker error: %s" % message)


func _on_chat_event_received(event: Dictionary) -> void:
    var ts: String = str(event.get("ts", ""))
    var stage: String = str(event.get("stage", "unknown-stage"))
    var status: String = str(event.get("status", "unknown-status"))
    var actor: String = str(event.get("actor", "unknown-actor"))
    var summary: String = str(event.get("summary", ""))

    var line: String = "[%s] %s | %s | %s | %s" % [ts, stage, status, actor, summary]
    _append_system_line(line)
    print("[ChatroomTest] %s" % line)


func _on_stage_status_changed(stage: String, status: String) -> void:
    var line: String = "Stage status changed: %s -> %s" % [stage, status]
    _append_system_line(line)
    print("[ChatroomTest] %s" % line)


func _on_status_snapshot_updated(summary: String) -> void:
    stage_snapshot_label.text = "Stage snapshot:\n%s" % summary


func _on_send_pressed() -> void:
    _submit_user_message(composer_input.text)


func _on_input_submitted(text: String) -> void:
    _submit_user_message(text)


func _on_input_text_changed(_new_text: String) -> void:
    _sync_send_button_state()


func _submit_user_message(raw_text: String) -> void:
    # Whitespace-only messages are ignored intentionally.
    # This keeps history clean and avoids low-value fake reply spam.
    # Status text explains why no action happened.
    var text: String = raw_text.strip_edges()
    if text.is_empty():
        _set_status("Status: type a message before sending")
        return

    _append_message(SPEAKER_USER, text)

    var next_speaker: String = _next_agent_speaker()
    _set_status("Status: %s is thinking..." % next_speaker)

    # Clearing input immediately confirms send succeeded.
    # Refocusing input supports fast keyboard-only chat loops.
    # Status changes communicate the fake-processing phase.
    composer_input.clear()
    composer_input.grab_focus()
    _sync_send_button_state()
    await get_tree().create_timer(FAKE_REPLY_DELAY_SEC).timeout
    var reply: String = _build_persona_reply(next_speaker, text)
    _append_message(next_speaker, reply)
    _set_status(STATUS_IDLE)


func _build_persona_reply(speaker: String, user_text: String) -> String:
    # Persona replies are intentionally local and deterministic.
    # This keeps behavior inside Phase 1's non-autonomous boundary.
    # Each speaker echoes their documented role in a short response.
    if speaker == SPEAKER_PIPER:
        return "Let's structure this: first define the goal, then pick one small next step for '%s'." % user_text
    if speaker == SPEAKER_MASON:
        return "Practical take: we can start with a minimal version around '%s' and iterate." % user_text
    if speaker == SPEAKER_VERA:
        return "Before we proceed with '%s', what could fail and how will we verify success?" % user_text
    if speaker == SPEAKER_TESS:
        return "Summary: we have direction on '%s'; let's pick one concrete action and move." % user_text
    return "I heard: %s" % user_text


func _next_agent_speaker() -> String:
    var speaker: String = AGENT_SPEAKERS[_agent_turn_index % AGENT_SPEAKERS.size()]
    _agent_turn_index += 1
    return speaker


func _append_message(speaker: String, text: String) -> void:
    # Single append path keeps in-memory state and rendered log aligned.
    # All send/reply/system paths should flow through this function.
    # That makes future persistence straightforward to add.
    var entry: Dictionary = {
        "speaker": speaker,
        "text": text,
        "ts": Time.get_datetime_string_from_system(true),
    }
    _conversation.append(entry)

    _render_events_from_conversation()
    _save_conversation_to_disk()


func _render_events_from_conversation() -> void:
    # The visual log is rendered from conversation state each time.
    # This enforces state-first UI behavior for Task 4.
    # Limiting to MAX_LINES prevents unbounded widget growth.
    for child in messages_list.get_children():
        child.queue_free()

    var start_index: int = max(_conversation.size() - MAX_LINES, 0)
    for i in range(start_index, _conversation.size()):
        var entry: Dictionary = _conversation[i]
        var role: String = str(entry.get("speaker", "unknown"))
        var body: String = str(entry.get("text", ""))
        var bubble: Node = MESSAGE_BUBBLE_SCENE.instantiate()
        bubble.call("configure", role, body)
        messages_list.add_child(bubble)

    call_deferred("_scroll_messages_to_bottom")


func _scroll_messages_to_bottom() -> void:
    # Deferred scroll waits for container size/layout refresh.
    # This keeps newest messages visible during fast append bursts.
    # It also avoids fragile frame-timing assumptions in callers.
    var bar: VScrollBar = messages_scroll.get_v_scroll_bar()
    if bar != null:
        messages_scroll.scroll_vertical = int(bar.max_value)


func _save_conversation_to_disk() -> void:
    # Persisting after each append keeps behavior simple for Phase 1.
    # The payload is intentionally plain JSON for easy inspection.
    # Write failures are surfaced in status and log, never as hard crashes.
    var handle: FileAccess = FileAccess.open(SAVE_FILE_PATH, FileAccess.WRITE)
    if handle == null:
        var err: int = FileAccess.get_open_error()
        _on_broker_error("save failed: %s" % err)
        return

    # We persist only user/assistant chat content.
    # Runtime system events are useful in-session but should not bloat history.
    # This keeps the saved file focused and readable.
    var persisted: Array[Dictionary] = []
    for entry in _conversation:
        var speaker: String = str(entry.get("speaker", ""))
        if speaker == SPEAKER_SYSTEM:
            continue
        persisted.append(entry)

    handle.store_string(JSON.stringify(persisted, "  "))


func _load_conversation_from_disk() -> void:
    # Startup should tolerate missing or malformed files safely.
    # We only admit entries that contain stable message fields.
    # Any parse/data issue falls back to an empty conversation model.
    if not FileAccess.file_exists(SAVE_FILE_PATH):
        return

    var handle: FileAccess = FileAccess.open(SAVE_FILE_PATH, FileAccess.READ)
    if handle == null:
        var err: int = FileAccess.get_open_error()
        _on_broker_error("load failed: %s" % err)
        return

    var raw: String = handle.get_as_text()
    if raw.strip_edges().is_empty():
        return

    var parsed: Variant = JSON.parse_string(raw)
    if typeof(parsed) != TYPE_ARRAY:
        _on_broker_error("load failed: invalid json shape")
        return

    var loaded: Array[Dictionary] = []
    for item in parsed:
        if typeof(item) != TYPE_DICTIONARY:
            continue

        var entry: Dictionary = item
        var role: String = str(entry.get("speaker", "")).strip_edges()
        if role.is_empty():
            # Backward compatibility for older saved shape.
            role = str(entry.get("role", "")).strip_edges()
            if role == "user":
                role = SPEAKER_USER
            elif role == "assistant":
                role = SPEAKER_PIPER
            elif role == "system":
                role = SPEAKER_SYSTEM

        var text: String = str(entry.get("text", "")).strip_edges()
        var ts: String = str(entry.get("ts", "")).strip_edges()
        if role == SPEAKER_SYSTEM:
            continue
        if role.is_empty() or text.is_empty():
            continue

        loaded.append({
            "speaker": role,
            "text": text,
            "ts": ts,
        })

    _conversation = loaded
    _render_events_from_conversation()


func _set_status(value: String) -> void:
    status_label.text = value


func _append_system_line(text: String) -> void:
    _append_message(SPEAKER_SYSTEM, text)


func _sync_send_button_state() -> void:
    # The button state follows trimmed input so UX matches validation.
    # We avoid enabling send for meaningless whitespace input.
    # This mirrors acceptance criteria around empty-message rejection.
    send_button.disabled = composer_input.text.strip_edges().is_empty()
