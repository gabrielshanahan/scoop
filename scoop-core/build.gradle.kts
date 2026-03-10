plugins { `java-library` }

dependencies {
    api(libs.postgresql)
    api(libs.jackson.databind)
    api(libs.jackson.module.kotlin)
    implementation(libs.fluentjdbc)
    implementation(libs.slf4j.api)
}
