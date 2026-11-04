# Console UI refresh

Implemented compact console styling, persistent dark/light theme, Records / Replay / Jobs / Connection tabs, bounded scroll areas, required-field markers, and hover/focus help. Errors and execution status remain visible.

Validation: existing 33 tests passed (zero failures/errors/skips); frontend TypeScript/Vite build passed; existing isolated packaged-JAR smoke passed; browser checked tabs, themes and frame layout. No tests added.

The original target JAR is locked by a running application. The updated distributable is release/replay-console-ui.jar.
Run: java -jar C:\Work\kafka_project\release\replay-console-ui.jar
SHA256: 9EAEC73479C49B54FB82B816A44ED742B7CD4E2E8287F109792A05C15E658FDA

The older Step 3 revision file describes the previous release.
