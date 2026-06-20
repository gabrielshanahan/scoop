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
    apply(plugin = "dev.detekt")
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

    configure<dev.detekt.gradle.extensions.DetektExtension> {
        buildUponDefaultConfig.set(true)
        config.setFrom(files("$rootDir/config/detekt/detekt.yml"))
    }

    // One baseline per module: detekt 2.0 no longer auto-wires the extension baseline onto the
    // Detekt tasks, and a single shared baseline file would be clobbered by whichever module runs
    // detektBaseline last. Set it explicitly on both the analysis and baseline-creation tasks.
    val moduleBaseline = file("$rootDir/config/detekt/baseline-$name.xml")

    tasks.withType<dev.detekt.gradle.DetektCreateBaselineTask>().configureEach {
        jvmTarget.set("21")
        baseline.set(moduleBaseline)
    }

    tasks.withType<dev.detekt.gradle.Detekt>().configureEach {
        jvmTarget.set("21")
        if (moduleBaseline.exists()) baseline.set(moduleBaseline)
        reports {
            html.required.set(true)
            sarif.required.set(false)
        }
    }

    tasks.named("check") { dependsOn("ktfmtCheck", "detekt") }

    tasks.register("format") { dependsOn("ktfmtFormat") }

    configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
        publishToMavenCentral()
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
