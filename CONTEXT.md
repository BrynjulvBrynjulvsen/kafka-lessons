# Current implementation

Split from the combined kafka-demo application on 2026-09-12. Kotlin sources live
under `src/main/kotlin/io/bekk/kafkalessons`. This is an independent Spring Boot build;
use `-PkafkaDemoCore=../kafka-demo` to consume the sibling library sources explicitly.
Core artifacts are `io.bekk.kafkademo:backend:0.1.0-SNAPSHOT` and
`io.bekk.kafkademo:presentation:0.1.0-SNAPSHOT`. Build both demos against the same core
revision until released artifacts establish independent version boundaries.

See README for current runtime configuration and docs/ADDING_LESSONS.md for extension
contracts. Runtime observations in the old core ADRs are historical, not proof of
current broker/container state. Validation of the split is recorded in the core's
CONTEXT.md. Runtime credentials were subsequently relocated into this project’s ignored `.local` directory.

## Standalone runtime transition, 2026-09-12

`compose.poc.yml` now runs the standalone application image with its own legacy
port-forward service. `scripts/build-image.sh` builds the JAR against core and copies
it into the image. No runtime mount references the core checkout or a build JAR.
Local credentials, kubeconfig and the optional forwarding binary belong to this
project's `.local` directory. See README for build/start/stop and Kind refresh steps.
The old combined runtime was stopped before reusing its observer groups, then removed
following successful live verification. The external POC was neither provisioned nor
migrated. Root core build artifacts and migration staging backups were removed.

Live runtime verification passed on 2026-09-12: four lesson browser checks, including real production, consumption and experiment controls. One Group A worker and zero delay were restored afterward.

2026-09-15: Added a lessons-local semantic color palette at
src/main/resources/static/css/palette.css, loaded after the core theme and before
slides.css. Lesson selectors now use semantic roles; compatibility aliases adapt
brand-named core selectors without modifying shared code or sibling demos.
Defaults preserve all existing colors. Static resolution checks found identical
color declarations for both core and lesson rules, no missing/circular variables,
and successful alternate accent/action/text/surface propagation. Stylesheet load
order verified. No browser rendering check or runtime rebuild/restart was performed.
