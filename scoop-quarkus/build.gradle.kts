plugins {
    `java-library`
    alias(libs.plugins.kotlin.allopen)
    alias(libs.plugins.quarkus)
}

dependencies {
    api(project(":scoop-core"))
    implementation(platform(libs.quarkus.bom))
    implementation(libs.quarkus.rest)
    implementation(libs.quarkus.flyway)
    implementation(libs.quarkus.rest.jackson)
    implementation(libs.quarkus.jackson.module.kotlin)
    implementation(libs.quarkus.jdbc.postgresql)
    implementation(libs.quarkus.fluentjdbc)
    implementation(libs.quarkus.reactive.pg.client)
    implementation(libs.quarkus.arc)
    implementation(libs.quarkus.kotlin)
    testImplementation(libs.quarkus.junit5)
}

allOpen {
    annotation("jakarta.ws.rs.Path")
    annotation("jakarta.enterprise.context.ApplicationScoped")
    annotation("io.quarkus.test.junit.QuarkusTest")
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}
