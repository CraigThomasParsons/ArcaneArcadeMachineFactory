Feature: Project Activation State Management

Status: Implemented MVP runtime controls.

Goal:
1. Keep one project active for focused testing.
2. Allow explicit switch of the active project.
3. Run stage pipeline against active project by default.

Commands:
1. python TheFactoryHopper/bin/stage_runtime.py list-projects
2. python TheFactoryHopper/bin/stage_runtime.py set-active-project --project <project-folder-name>
3. python TheFactoryHopper/bin/stage_runtime.py run-once

State file:
1. TheFactoryHopper/docs/active_project.json

Behavior:
1. run-once uses --project when provided.
2. Otherwise run-once uses active_project.json.
3. If no active project is set, first inbox project is used as fallback.