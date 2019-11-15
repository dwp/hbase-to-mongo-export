import net.researchgate.release.GitAdapter
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.springframework.boot.gradle.tasks.run.BootRun

plugins {
    id("org.springframework.boot") version "2.1.7.RELEASE"
    id("io.spring.dependency-management") version "1.0.7.RELEASE"
    kotlin("jvm") version "1.3.40"
    kotlin("plugin.spring") version "1.3.40"
    id ("net.researchgate.release") version "2.6.0"
    application
    groovy
}

group = "uk.gov.dwp.dataworks"
java.sourceCompatibility = JavaVersion.VERSION_1_8

repositories {
    mavenCentral()
}

tasks.bootJar {
    launchScript()
}

release {
    failOnPublishNeeded = false
    with (propertyMissing("git") as GitAdapter.GitConfig) {
        requireBranch = ""
    }
}

dependencies {
    // See https://github.com/aws/aws-sdk-java-v2
    // See https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/java/example_code/s3/src/main/java/CopyObjectSingleOperation.java
    //implementation("software.amazon.awssdk:aws-sdk-java:2.7.16")

    // sdk v1
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.603")
    implementation("com.amazonaws:aws-java-sdk-core:1.11.603")

    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-batch")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.apache.hbase:hbase-client:2.2.0")
    implementation("com.google.code.gson:gson:2.8.5")
    implementation("org.apache.commons:commons-compress:1.5")
    implementation("org.bouncycastle:bcprov-ext-jdk15on:1.62")
    implementation("commons-codec:commons-codec:1.12")
    implementation("org.spockframework:spock-core:1.3-groovy-2.5")
    implementation("junit:junit:4.12")
    implementation("org.apache.httpcomponents:fluent-hc:4.5.6")
    implementation("org.apache.httpcomponents:httpclient:4.5.9")
    implementation("org.springframework.retry:spring-retry")
    implementation("org.springframework.boot:spring-boot-starter-aop")
    implementation("org.apache.commons:commons-text:1.8")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.batch:spring-batch-test")
    testImplementation("com.nhaarman.mockitokotlin2", "mockito-kotlin", "2.2.0")
    testImplementation("io.kotlintest", "kotlintest-runner-junit5", "3.3.2")
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "1.8"
    }
}

application {
    mainClassName = "app.HBaseToMongoExportKt"
}

tasks.getByName<BootRun>("bootRun") {
    main = "app.HBaseToMongoExportKt"
    systemProperties = properties
}

sourceSets {
    create("integration") {
        java.srcDir(file("src/integration/groovy"))
        java.srcDir(file("src/integration/kotlin"))
        compileClasspath += sourceSets.getByName("main").output + configurations.testRuntimeClasspath
        runtimeClasspath += output + compileClasspath
    }
}

tasks.register<Test>("integration") {
    description = "Runs the integration tests."
    group = "verification"
    testClassesDirs = sourceSets["integration"].output.classesDirs
    classpath = sourceSets["integration"].runtimeClasspath
    testLogging {
        exceptionFormat = TestExceptionFormat.FULL
        events = setOf(TestLogEvent.PASSED, TestLogEvent.FAILED, TestLogEvent.SKIPPED, TestLogEvent.STANDARD_OUT)
    }
}
