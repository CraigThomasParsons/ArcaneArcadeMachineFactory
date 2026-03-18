## Contracts

### Input Contract
- Must receive at least one conversation file
- Files must be markdown

### Output Contract
- Must produce:
  - vision.md
  - personas.md
  - at least 3 stories

### Retry Policy
- Retry up to 2 times on malformed output

### Validation Rules
- No empty sections
- Stories must follow:
  - As a [persona]
  - I want [goal]
  - So that [outcome]