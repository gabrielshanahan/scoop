plugins { `java-library` }

dependencies {
    api(libs.postgresql)
    api(libs.jackson.databind)
    api(libs.jackson.module.kotlin)
    api(libs.fluentjdbc)
    implementation(libs.slf4j.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    // JUnit 5.14 (Platform 1.14) needs a matching launcher on the test runtime classpath; Gradle's
    // bundled one is too old and fails discovery ("OutputDirectoryCreator not available").
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation(libs.jackson.module.kotlin)
}
