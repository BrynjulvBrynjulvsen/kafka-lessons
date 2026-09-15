rootProject.name = "kafka-lessons"
providers.gradleProperty("kafkaDemoCore").orNull?.let { path ->
    require(file(path).resolve("settings.gradle.kts").isFile) { "Missing Kafka core build: $path" }
    includeBuild(path)
}
