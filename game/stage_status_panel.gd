extends Control
class_name StageStatusPanel

signal status_snapshot_updated(summary_text)

@export var bridge_path: NodePath
@export var stage_status_file: String = "/home/craigpar/Code/ArcaneArcadeMachineFactory/TheFactoryHopper/docs/stage_status.json"
@export var poll_interval_sec: float = 1.0

var _bridge: ChatroomBridge
var _stage_state: Dictionary = {}
var _last_mtime: int = -1
var _poll_accum: float = 0.0

func _ready() -> void:
    if bridge_path == NodePath():
        return

    _bridge = get_node(bridge_path)
    if _bridge == null:
        return

    _bridge.stage_status_changed.connect(_on_stage_status_changed)
    _bridge.broker_connected.connect(_on_broker_connected)
    _bridge.broker_disconnected.connect(_on_broker_disconnected)

    _poll_status_snapshot()


func _process(delta: float) -> void:
    _poll_accum += delta
    if _poll_accum < poll_interval_sec:
        return
    _poll_accum = 0.0
    _poll_status_snapshot()


func _on_stage_status_changed(stage: String, status: String) -> void:
    _stage_state[stage] = status
    _render_status()


func _on_broker_connected() -> void:
    print("[StageStatusPanel] broker connected")


func _on_broker_disconnected() -> void:
    print("[StageStatusPanel] broker disconnected")


func _render_status() -> void:
    print("[StageStatusPanel] current status:", _stage_state)


func _poll_status_snapshot() -> void:
    if stage_status_file == "":
        return

    if not FileAccess.file_exists(stage_status_file):
        return

    var mtime: int = FileAccess.get_modified_time(stage_status_file)
    if mtime <= 0 or mtime == _last_mtime:
        return

    _last_mtime = mtime
    var file := FileAccess.open(stage_status_file, FileAccess.READ)
    if file == null:
        return

    var raw: String = file.get_as_text()
    var parsed: Variant = JSON.parse_string(raw)
    if typeof(parsed) != TYPE_DICTIONARY:
        return

    var status_doc: Dictionary = parsed
    var stages: Dictionary = status_doc.get("stages", {})
    if stages.is_empty():
        return

    var names := stages.keys()
    names.sort()

    var lines: Array[String] = []
    for name in names:
        var data: Dictionary = stages.get(name, {})
        var state: String = str(data.get("status", "unknown"))
        var details: Dictionary = data.get("details", {})
        var queue_depth: String = str(details.get("queue_depth", "n/a"))
        var last_job: String = str(details.get("last_job_id", "n/a"))
        lines.append("%s | %s | q=%s | job=%s" % [str(name), state, queue_depth, last_job])

    var summary: String = "\n".join(lines)
    emit_signal("status_snapshot_updated", summary)
