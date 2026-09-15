# Set up the Kafka lessons

Run Kafka in Docker and the lesson application on your computer, so you can edit
producer and consumer code and restart it easily. Each participant runs their own
broker and slides. No workshop stack, migration demo, Kubernetes or credentials
are needed.

## Have these ready before the session

- **Docker Desktop**, or Docker Engine with **Docker Compose v2**, running.
- **JDK 21**. Set `JAVA_HOME` to it; check that `java -version` reports 21.
- **IntelliJ IDEA with Kotlin support**, or another Kotlin-capable IDE. Select
  JDK 21 for both the project SDK and Gradle JVM.
- **A modern browser** and free local ports **8080** (slides), **8081** (Kafbat UI), and **9094** (Kafka).
- **The lesson and core source folders**, supplied by the facilitator. If cloning
  them, install Git too. Keep the supplied matching revisions together.
- **Internet access for the first run**, to download the Kafka image, Gradle and
  Java dependencies. Complete the readiness check below before arriving.

You do not need to install Gradle, a standalone Kotlin compiler, Node.js, or Kafka
on your computer. The Gradle wrapper supplies the build tooling and Kotlin compiler.

## 1. Arrange the source folders

Unpack or clone the supplied sources side by side:

```text
your-workspace/
  kafka-demo/       # shared libraries
  kafka-lessons/    # application and slides; open this folder in IntelliJ
```

Run the following commands in a terminal inside `kafka-lessons`.
On Windows, replace `./gradlew` with `.\gradlew.bat`; keep the other arguments.

### Select Java with SDKMAN (optional)

If you use [SDKMAN](https://sdkman.io/install/), the included `.sdkmanrc` selects
Temurin JDK 21.0.7, matching the build's Java 21 toolchain. From `kafka-lessons`:

```sh
sdk env install   # first time, if the pinned JDK is not installed
sdk env           # select the project's JDK in this shell
java -version
```

Run `sdk env` whenever you open a new terminal in this project. For automatic
selection when entering the directory, set `sdkman_auto_env=true` in
`~/.sdkman/etc/config`. The IDE's project SDK and Gradle JVM still need to be
configured separately as described above.

## 2. Start Kafka and create the topics

```sh
docker compose up -d kafka kafbat-ui
docker compose run --rm init-topics
```

Wait for the second command to finish successfully. It waits for Kafka to become
healthy and creates `kafka-demo` and `kafka-demo-lab`, each with three partitions.
Both commands are safe to repeat; existing records and topics are preserved.

Open [Kafbat UI](http://localhost:8081), select **lessons → Topics → kafka-demo → Messages**,
and use String deserialization for the demo key and value. The UI is configured to
connect to this Compose broker at `kafka:9092`.

## 3. Start the slides

```sh
./gradlew -PkafkaDemoCore=../kafka-demo bootRun
```

Leave this terminal running. Open **http://localhost:8080**. Wait for the backend
log to show the observer's partition assignment before sending records.

### Readiness check

Go to the partitioning experiment, send a record with key `customer-1`, and confirm
you see both its acknowledgment (partition and offset) and its consumed-record
card. That checks the browser, application, producer, broker and consumer together.

## 4. Edit and run code

Open `kafka-lessons` as a Gradle project in IntelliJ. To let IDE Gradle imports
resolve the sibling libraries, create an untracked `gradle.properties` in
`kafka-lessons` containing:

```properties
kafkaDemoCore=../kafka-demo
```

Reload the Gradle project. Run its `bootRun` task from the Gradle tool window, or
keep using the terminal command above. Stop the previous application before
starting another. After code changes, stop and rerun it; Kafka can stay running.

Useful starting points:

- **Send records:** `kafka-demo/backend/src/main/kotlin/io/bekk/kafkademo/core/MessageController.kt`
  uses `KafkaTemplate`.
- **Poll, process and commit:** `src/main/kotlin/io/bekk/kafkalessons/ExperimentRuntime.kt`
  contains the experiment `Worker` and its `KafkaConsumer` loop.
- **Slide content:** `src/main/resources/static/index.html`.

Change one thing, predict its effect, rerun and compare the observations. Standalone
producer/consumer exercises can use `localhost:9094`, string keys/values, and the
same topics; give your own consumer a separate group ID.

## Stop or start fresh

Stop the application with Ctrl+C (or the IDE Stop button), then stop Kafka:

```sh
docker compose down
```

Records and consumer offsets are retained for next time. To **delete this local
lesson broker's records and offsets**, stop the application and run:

```sh
docker compose down -v
```

Repeat steps 2 and 3 to start fresh.

## If something fails

| Symptom | Check |
| --- | --- |
| Docker connection error | Start Docker and wait until its engine is ready. |
| Port already in use | Stop the other application using 8080, Kafbat UI using 8081, or broker using 9094, including an old workshop setup. |
| Java/Gradle version error | Select JDK 21 in `JAVA_HOME` and the IDE's Gradle JVM. |
| Cannot resolve core dependencies | Check the sibling directory names and the `-PkafkaDemoCore=../kafka-demo` argument or IDE property above. |
| Kafka unavailable or missing topics | Run `docker compose ps`, inspect `docker compose logs kafka`, and rerun `docker compose run --rm init-topics`. |
| Send succeeds but no card appears | Wait for observer assignment; use one lesson application instance. |

Use a fresh terminal without settings from another Kafka environment (especially
`SPRING_PROFILES_ACTIVE`, `KAFKA_BOOTSTRAP_SERVERS` or `KAFKA_CLIENT_PROPERTIES`).
The default lesson settings work with this Compose setup. See [README.md](README.md)
for advanced configuration and experiment details.
