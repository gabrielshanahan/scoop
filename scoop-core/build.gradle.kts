plugins { `java-library` }

dependencies {
    api(libs.postgresql)
    api(libs.jackson.databind)
    api(libs.jackson.module.kotlin)
    api(libs.fluentjdbc)
    implementation(libs.slf4j.api)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.jackson.module.kotlin)
}
