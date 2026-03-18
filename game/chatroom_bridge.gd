extends Node
class_name ChatroomBridge

signal broker_connected
signal broker_disconnected
signal broker_error(message)
signal chat_event_received(event)
signal stage_status_changed(stage, status)

@export var broker_url: String = "ws://127.0.0.1:8765"
@export var channel: String = "factory.chatroom"
@export var reconnect_delay_sec: float = 2.0

var _socket: WebSocketPeer = WebSocketPeer.new()
var _is_connected: bool = false
var _last_seen_event_id: String = ""
var _reconnect_deadline_ms: int = 0

func _ready() -> void:
    _connect_broker()


func _process(_delta: float) -> void:
    var now_ms: int = Time.get_ticks_msec()
    var state_before_poll: int = _socket.get_ready_state()

    # Reconnect attempts should only happen from closed state.
    # Calling connect while the socket is connecting/open causes ERR_ALREADY_IN_USE.
    # This guard keeps retry logic deterministic and avoids noisy error loops.
    if not _is_connected and state_before_poll == WebSocketPeer.STATE_CLOSED and now_ms >= _reconnect_deadline_ms:
        _connect_broker()

    _socket.poll()

    var state: int = _socket.get_ready_state()
    if state == WebSocketPeer.STATE_OPEN:
        if not _is_connected:
            _is_connected = true
            emit_signal("broker_connected")
            _send_subscribe()
        _drain_messages()
    elif state == WebSocketPeer.STATE_CLOSED:
        if _is_connected:
            _is_connected = false
            emit_signal("broker_disconnected")
        _reconnect_deadline_ms = now_ms + int(reconnect_delay_sec * 1000.0)


func _connect_broker() -> void:
    # Defensive gate: skip if socket is already connecting or open.
    # This protects against duplicate connect calls in rapid frame loops.
    # A valid reconnect attempt should always start from closed state.
    var state: int = _socket.get_ready_state()
    if state != WebSocketPeer.STATE_CLOSED:
        return

    var err: int = _socket.connect_to_url(broker_url)
    if err != OK:
        emit_signal("broker_error", "connect failed: %s" % err)
        _reconnect_deadline_ms = Time.get_ticks_msec() + int(reconnect_delay_sec * 1000.0)


func _send_subscribe() -> void:
    var subscribe := {
        "cmd": "subscribe",
        "channel": channel,
        "last_seen_event_id": _last_seen_event_id,
    }
    _socket.send_text(JSON.stringify(subscribe))


func _drain_messages() -> void:
    while _socket.get_available_packet_count() > 0:
        var packet: PackedByteArray = _socket.get_packet()
        var text := packet.get_string_from_utf8()
        var parsed: Variant = JSON.parse_string(text)
        if typeof(parsed) != TYPE_DICTIONARY:
            emit_signal("broker_error", "invalid packet json")
            continue

        var msg: Dictionary = parsed
        var msg_type: String = str(msg.get("type", ""))

        if msg_type == "error":
            emit_signal("broker_error", str(msg.get("message", "unknown error")))
            continue

        if msg_type == "event":
            var event: Dictionary = msg.get("event", {})
            if event.is_empty():
                continue

            _last_seen_event_id = str(event.get("event_id", _last_seen_event_id))
            emit_signal("chat_event_received", event)

            var stage: String = str(event.get("stage", ""))
            var status: String = str(event.get("status", ""))
            if stage != "" and status != "":
                emit_signal("stage_status_changed", stage, status)


func publish_event(event: Dictionary) -> void:
    if _socket.get_ready_state() != WebSocketPeer.STATE_OPEN:
        emit_signal("broker_error", "publish failed: broker not connected")
        return

    var envelope := {
        "cmd": "publish",
        "channel": channel,
        "event": event,
    }
    _socket.send_text(JSON.stringify(envelope))
