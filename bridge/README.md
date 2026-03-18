# WebSocket Bridge MVP

This folder contains the local WebSocket broker and publisher utilities used to stream stage/chatroom events into Godot.

## Files

1. ws_broker.py: local broker with subscribe, publish, replay, and JSONL audit append.
2. publish_event.py: one-shot publisher utility for worker agents and manual testing.
3. ws_event_schema.json: event payload schema contract.
4. requirements.txt: Python dependency list.

## Install

1. Create/activate a Python environment.
2. Install dependencies:

   pip install -r bridge/requirements.txt

## Run Broker

1. Start broker:

   python bridge/ws_broker.py --host 127.0.0.1 --port 8765

2. Expected startup line:

   [ws-broker] listening on ws://127.0.0.1:8765

## Publish Test Event

Use a second terminal:

python bridge/publish_event.py \
  --channel factory.chatroom \
  --stage writers-room \
  --actor WriterAgent \
  --event artifact_transformed \
  --summary "conversation to vision complete" \
  --status ok \
  --artifact-ref "inbox/conversation.json" \
  --artifact-ref "outbox/vision.md"

## Godot Wiring

1. Add ChatroomBridge node with script game/chatroom_bridge.gd.
2. Add StageStatusPanel node with script game/stage_status_panel.gd.
3. Set StageStatusPanel.bridge_path to ChatroomBridge node path.
4. Run scene and publish an event; observe StageStatusPanel output.

## Minimal MVP Test Scene

A ready-to-run scene is included for quick validation:

1. Scene: `game/chatroom_test_scene.tscn`
2. Controller script: `game/chatroom_test_scene.gd`

What it does:

1. Wires `ChatroomBridge` and `StageStatusPanel`.
2. Prints live incoming event summaries to Godot output.
3. Shows connection state and event log in the scene UI.

How to run:

1. Start the broker.
2. Open `game/chatroom_test_scene.tscn` in Godot and run it.
3. Publish a test event from terminal.
4. Verify event appears in both Godot console and on-screen log.

## Reliability Notes

1. Broker keeps in-memory replay buffer per channel.
2. Godot tracks last_seen_event_id and can replay after reconnect.
3. Broker appends events to audit JSONL in TheFactoryHopper/chatroom_ws_audit.
