extends SceneTree

const GRAPH_PATH := "user://memory_graph.project_demo.json"
const TMP_PATH := "user://memory_graph.project_demo.json.tmp"
const BACKUP_PATH := "user://memory_graph.project_demo.json.bak"
const INVALID_MARKER_PATH := "user://memory_graph.project_demo.json.invalid"

const PRIORITY_RANK := {
    "CRITICAL": 4,
    "HIGH": 3,
    "MEDIUM": 2,
    "LOW": 1,
}

func _initialize() -> void:
    print("[BOOT] Sprint2 Task4 hardening start")

    var suite_ok: bool = _run_validation_suite()
    if not suite_ok:
        print("[SUITE] failed")
        quit(3)
        return

    print("[SUITE] passed")
    print("[BOOT] Sprint2 Task4 hardening complete")
    quit(0)


func _run_validation_suite() -> bool:
    var valid_case_ok: bool = _test_valid_resume_and_determinism()
    var parse_case_ok: bool = _test_parse_failure_marker()
    var missing_dep_ok: bool = _test_missing_dependency_detection()
    var cycle_ok: bool = _test_cycle_detection()

    print("[RESULT] valid_case=", valid_case_ok)
    print("[RESULT] parse_failure_case=", parse_case_ok)
    print("[RESULT] missing_dependency_case=", missing_dep_ok)
    print("[RESULT] cycle_case=", cycle_ok)

    return valid_case_ok and parse_case_ok and missing_dep_ok and cycle_ok


func _test_valid_resume_and_determinism() -> bool:
    print("[CASE] valid_resume_and_determinism")

    var seeded: Dictionary = _seed_graph()
    var save_ok: bool = _save_graph(seeded)
    print("[STORE] save status=", save_ok)
    if not save_ok:
        return false

    var loaded_result: Dictionary = _load_graph()
    print("[LOAD] status=", loaded_result.get("status", "UNKNOWN"))
    if loaded_result.get("status", "") != "OK":
        return false

    var loaded_graph: Dictionary = loaded_result.get("graph", {})
    var validation: Dictionary = _validate_graph(loaded_graph)
    print("[VALIDATE] valid=", validation.get("valid", false), " errors=", validation.get("errors", []))
    if not validation.get("valid", false):
        return false

    var first: String = _resolve_next_task(loaded_graph)
    var second: String = _resolve_next_task(loaded_graph)
    print("[RESOLVE] first=", first, " second=", second)

    var deterministic: bool = first == second
    print("[CHECK] deterministic=", deterministic)
    if not deterministic:
        return false

    var nodes: Array = loaded_graph.get("nodes", [])
    if nodes.size() > 0 and typeof(nodes[0]) == TYPE_DICTIONARY:
        var first_node: Dictionary = nodes[0]
        first_node["status"] = "DONE"
        nodes[0] = first_node
        loaded_graph["nodes"] = nodes
    var second_save_ok: bool = _save_graph(loaded_graph)
    if not second_save_ok:
        return false

    var after_done: String = _resolve_next_task(loaded_graph)
    print("[RESOLVE] after_done=", after_done)
    if after_done != "task-b":
        return false

    return true


func _test_parse_failure_marker() -> bool:
    print("[CASE] parse_failure_marker")
    var bad_write_ok: bool = _safe_write_text(GRAPH_PATH, "{ definitely-not-json")
    if not bad_write_ok:
        return false

    var load_result: Dictionary = _load_graph()
    var status: String = str(load_result.get("status", "UNKNOWN"))
    print("[LOAD] status=", status)
    if status != "PARSE_FAILURE":
        return false

    var marker_exists: bool = FileAccess.file_exists(INVALID_MARKER_PATH)
    print("[CHECK] invalid_marker_exists=", marker_exists)
    return marker_exists


func _test_missing_dependency_detection() -> bool:
    print("[CASE] missing_dependency_detection")
    var graph: Dictionary = _seed_graph()
    var nodes: Array = graph.get("nodes", [])
    if nodes.size() == 0:
        return false

    var node0: Dictionary = nodes[0]
    node0["depends_on"] = ["ghost-task"]
    nodes[0] = node0
    graph["nodes"] = nodes

    var report: Dictionary = _validate_graph(graph)
    var valid: bool = bool(report.get("valid", false))
    print("[VALIDATE] valid=", valid, " errors=", report.get("errors", []))
    return not valid


func _test_cycle_detection() -> bool:
    print("[CASE] cycle_detection")
    var graph: Dictionary = _seed_graph()
    var nodes: Array = graph.get("nodes", [])
    if nodes.size() < 2:
        return false

    var node0: Dictionary = nodes[0]
    var node1: Dictionary = nodes[1]
    node0["depends_on"] = ["task-b"]
    node1["depends_on"] = ["task-a"]
    nodes[0] = node0
    nodes[1] = node1
    graph["nodes"] = nodes

    var report: Dictionary = _validate_graph(graph)
    var valid: bool = bool(report.get("valid", false))
    print("[VALIDATE] valid=", valid, " errors=", report.get("errors", []))
    return not valid



func _seed_graph() -> Dictionary:
    var now: String = "2026-03-16T00:00:00Z"
    return {
        "version": "v0",
        "project_id": "demo",
        "generated_at": now,
        "nodes": [
            {
                "id": "task-a",
                "status": "TODO",
                "priority": "HIGH",
                "depends_on": [],
                "created_at": "2026-03-16T00:00:01Z",
                "updated_at": "2026-03-16T00:00:01Z",
                "blockers": [],
            },
            {
                "id": "task-b",
                "status": "TODO",
                "priority": "HIGH",
                "depends_on": ["task-a"],
                "created_at": "2026-03-16T00:00:02Z",
                "updated_at": "2026-03-16T00:00:02Z",
                "blockers": [],
            },
            {
                "id": "task-c",
                "status": "TODO",
                "priority": "MEDIUM",
                "depends_on": [],
                "created_at": "2026-03-16T00:00:03Z",
                "updated_at": "2026-03-16T00:00:03Z",
                "blockers": [],
            },
        ],
    }


func _save_graph(graph: Dictionary) -> bool:
    var ordered: Dictionary = _canonicalize_graph(graph)
    var json: String = JSON.stringify(ordered, "\t", false)
    var tmp_file := FileAccess.open(TMP_PATH, FileAccess.WRITE)
    if tmp_file == null:
        return false
    tmp_file.store_string(json)
    tmp_file.flush()
    tmp_file.close()

    var graph_abs: String = ProjectSettings.globalize_path(GRAPH_PATH)
    var tmp_abs: String = ProjectSettings.globalize_path(TMP_PATH)
    var backup_abs: String = ProjectSettings.globalize_path(BACKUP_PATH)

    if FileAccess.file_exists(graph_abs):
        DirAccess.remove_absolute(backup_abs)
        var backup_err: Error = DirAccess.rename_absolute(graph_abs, backup_abs)
        if backup_err != OK:
            return false

    var swap_err: Error = DirAccess.rename_absolute(tmp_abs, graph_abs)
    if swap_err != OK:
        return false

    return true


func _safe_write_text(path: String, content: String) -> bool:
    var file := FileAccess.open(path, FileAccess.WRITE)
    if file == null:
        return false
    file.store_string(content)
    file.flush()
    file.close()
    return true


func _load_graph() -> Dictionary:
    if not FileAccess.file_exists(GRAPH_PATH):
        return {"status": "FILE_NOT_FOUND"}

    var file := FileAccess.open(GRAPH_PATH, FileAccess.READ)
    if file == null:
        return {"status": "IO_FAILURE"}

    var raw: String = file.get_as_text()
    file.close()

    var parsed: Variant = JSON.parse_string(raw)
    if typeof(parsed) != TYPE_DICTIONARY:
        _safe_write_text(INVALID_MARKER_PATH, "parse_failure")
        return {"status": "PARSE_FAILURE"}

    return {
        "status": "OK",
        "graph": parsed,
    }


func _validate_graph(graph: Dictionary) -> Dictionary:
    var errors: Array = []
    if not graph.has("version"):
        errors.append("missing version")
    if not graph.has("nodes") or typeof(graph.nodes) != TYPE_ARRAY:
        errors.append("missing nodes")

    var ids: Dictionary = {}
    for node in graph.get("nodes", []):
        if typeof(node) != TYPE_DICTIONARY:
            errors.append("node is not a dictionary")
            continue

        var node_dict: Dictionary = node
        var id: String = str(node_dict.get("id", ""))
        if id == "":
            errors.append("node with empty id")
            continue
        if ids.has(id):
            errors.append("duplicate id: " + id)
        ids[id] = true

        if not PRIORITY_RANK.has(str(node_dict.get("priority", ""))):
            errors.append("invalid priority for " + id)
        if str(node_dict.get("status", "")) not in ["TODO", "IN_PROGRESS", "BLOCKED", "REVIEW", "DONE"]:
            errors.append("invalid status for " + id)

        for dep in node_dict.get("depends_on", []):
            if dep == id:
                errors.append("self dependency: " + id)

    for node in graph.get("nodes", []):
        if typeof(node) != TYPE_DICTIONARY:
            continue
        var node_dict: Dictionary = node
        var node_id: String = str(node_dict.get("id", ""))
        for dep in node_dict.get("depends_on", []):
            if not ids.has(dep):
                errors.append("missing dependency for " + node_id + " -> " + str(dep))

    if _has_cycle(graph):
        errors.append("cycle detected in depends_on graph")

    return {
        "valid": errors.is_empty(),
        "errors": errors,
    }


func _resolve_next_task(graph: Dictionary) -> String:
    var nodes: Array = graph.get("nodes", [])
    var status_by_id: Dictionary = {}
    for node in nodes:
        if typeof(node) != TYPE_DICTIONARY:
            continue
        var node_dict: Dictionary = node
        status_by_id[str(node_dict.get("id", ""))] = str(node_dict.get("status", ""))

    var candidates: Array = []
    for node in nodes:
        if typeof(node) != TYPE_DICTIONARY:
            continue

        var node_dict: Dictionary = node
        if str(node_dict.get("status", "")) != "TODO":
            continue
        if node_dict.get("blockers", []).size() > 0:
            continue

        var deps_met: bool = true
        for dep in node_dict.get("depends_on", []):
            if status_by_id.get(str(dep), "") != "DONE":
                deps_met = false
                break
        if deps_met:
            candidates.append(node_dict)

    if candidates.is_empty():
        return "NO_ELIGIBLE_TASK"

    candidates.sort_custom(_candidate_sort)
    var selected: Dictionary = candidates[0]
    return str(selected.get("id", "NO_ELIGIBLE_TASK"))


func _candidate_sort(a: Dictionary, b: Dictionary) -> bool:
    var pa: int = int(PRIORITY_RANK.get(str(a.get("priority", "")), 0))
    var pb: int = int(PRIORITY_RANK.get(str(b.get("priority", "")), 0))
    if pa != pb:
        return pa > pb

    var a_created: String = str(a.get("created_at", ""))
    var b_created: String = str(b.get("created_at", ""))
    if a_created != b_created:
        return a_created < b_created

    return str(a.get("id", "")) < str(b.get("id", ""))


func _canonicalize_graph(graph: Dictionary) -> Dictionary:
    var copy: Dictionary = graph.duplicate(true)

    var nodes: Array = copy.get("nodes", [])
    nodes.sort_custom(func(a: Dictionary, b: Dictionary) -> bool:
        return str(a.get("id", "")) < str(b.get("id", ""))
    )

    for i in range(nodes.size()):
        var deps: Array = nodes[i].get("depends_on", [])
        deps.sort()
        nodes[i]["depends_on"] = deps

        if nodes[i].has("blockers"):
            var blockers: Array = nodes[i].get("blockers", [])
            blockers.sort()
            nodes[i]["blockers"] = blockers

    copy["nodes"] = nodes
    return copy


func _has_cycle(graph: Dictionary) -> bool:
    var nodes: Array = graph.get("nodes", [])
    var adjacency: Dictionary = {}
    for node in nodes:
        if typeof(node) != TYPE_DICTIONARY:
            continue
        var node_dict: Dictionary = node
        var node_id: String = str(node_dict.get("id", ""))
        adjacency[node_id] = node_dict.get("depends_on", [])

    var state: Dictionary = {}
    for node_id in adjacency.keys():
        state[node_id] = 0

    for node_id in adjacency.keys():
        if int(state[node_id]) == 0 and _dfs_cycle(node_id, adjacency, state):
            return true
    return false


func _dfs_cycle(node_id: String, adjacency: Dictionary, state: Dictionary) -> bool:
    state[node_id] = 1
    var deps: Array = adjacency.get(node_id, [])
    for dep in deps:
        var dep_id: String = str(dep)
        if not state.has(dep_id):
            continue
        if int(state[dep_id]) == 1:
            return true
        if int(state[dep_id]) == 0 and _dfs_cycle(dep_id, adjacency, state):
            return true
    state[node_id] = 2
    return false
