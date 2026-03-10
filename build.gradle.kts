plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.allopen) apply false
    alias(libs.plugins.quarkus) apply false
    alias(libs.plugins.ktfmt) apply false
    alias(libs.plugins.detekt) apply false
    alias(libs.plugins.maven.publish) apply false
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "com.ncorti.ktfmt.gradle")
    apply(plugin = "io.gitlab.arturbosch.detekt")
    apply(plugin = "com.vanniktech.maven.publish")

    group = "io.github.gabrielshanahan"
    version = rootProject.version

    repositories {
        mavenCentral()
        mavenLocal()
    }

    configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
            javaParameters.set(true)
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        options.compilerArgs.add("-parameters")
    }

    configure<com.ncorti.ktfmt.gradle.KtfmtExtension> {
        kotlinLangStyle()
    }

    configure<io.gitlab.arturbosch.detekt.extensions.DetektExtension> {
        buildUponDefaultConfig = true
        config.setFrom(files("$rootDir/config/detekt/detekt.yml"))
        baseline = file("$rootDir/config/detekt/baseline.xml")
    }

    tasks.withType<io.gitlab.arturbosch.detekt.DetektCreateBaselineTask>().configureEach {
        jvmTarget = "21"
    }

    tasks.withType<io.gitlab.arturbosch.detekt.Detekt>().configureEach {
        jvmTarget = "21"
        reports {
            html.required.set(true)
            xml.required.set(false)
            txt.required.set(true)
            sarif.required.set(false)
            md.required.set(false)
        }
    }

    tasks.named("check") { dependsOn("ktfmtCheck", "detekt") }

    tasks.register("format") { dependsOn("ktfmtFormat") }

    configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
        publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
        if (project.findProperty("signingInMemoryKey") != null ||
            project.findProperty("signing.keyId") != null) {
            signAllPublications()
        }

        coordinates(project.group.toString(), project.name, project.version.toString())

        pom {
            name.set(project.name)
            description.set("Structured Cooperation for distributed systems")
            url.set("https://github.com/gabrielshanahan/scoop")
            licenses {
                license {
                    name.set("BSD-3-Clause")
                    url.set("https://opensource.org/licenses/BSD-3-Clause")
                }
            }
            developers {
                developer {
                    id.set("gabrielshanahan")
                    name.set("Gabriel Shanahan")
                }
            }
            scm {
                url.set("https://github.com/gabrielshanahan/scoop")
                connection.set("scm:git:git://github.com/gabrielshanahan/scoop.git")
                developerConnection.set(
                    "scm:git:ssh://github.com/gabrielshanahan/scoop.git"
                )
            }
        }
    }
}
