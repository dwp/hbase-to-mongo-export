import net.researchgate.release.GitAdapter
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.springframework.boot.gradle.tasks.run.BootRun

plugins {
    id("org.springframework.boot") version "2.3.0.RELEASE"
    id("io.spring.dependency-management") version "1.0.9.RELEASE"
    kotlin("jvm") version "1.4.10"
    kotlin("plugin.spring") version "1.4.10"
    id ("net.researchgate.release") version "2.6.0"
    application
    groovy
}

group = "uk.gov.dwp.dataworks"
java.sourceCompatibility = JavaVersion.VERSION_1_8

repositories {
    mavenCentral()
    jcenter()
    maven(url = "https://jitpack.io")
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

configurations.all {
    exclude(group="org.slf4j", module="slf4j-log4j12")
}

dependencies {
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.706")
    implementation("com.amazonaws:aws-java-sdk-core:1.11.706")
    implementation("com.amazonaws:aws-java-sdk-dynamodb:1.11.706")
    implementation("com.amazonaws:aws-java-sdk-sqs:1.11.706")

    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-batch:2.2.0.RELEASE")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.apache.hbase:hbase-client:1.4.13")
    implementation("com.google.code.gson:gson:2.8.5")
    implementation("org.apache.commons:commons-compress:1.19")
    implementation("org.bouncycastle:bcprov-ext-jdk15on:1.62")
    implementation("org.spockframework:spock-core:1.3-groovy-2.5")
    implementation("junit:junit:4.12")
    implementation("org.apache.httpcomponents:fluent-hc:4.5.6")
    implementation("org.apache.httpcomponents:httpclient:4.5.9")
    implementation("org.springframework.retry:spring-retry")
    implementation("org.springframework.boot:spring-boot-starter-aop")
    implementation("org.apache.commons:commons-text:1.8")
    implementation("com.beust:klaxon:4.0.2")
    implementation("com.github.dwp:dataworks-common-logging:0.0.6")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.batch:spring-batch-test:4.2.0.RELEASE")
    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.2.0")
    testImplementation("io.kotest:kotest-runner-junit5-jvm:4.2.0")
    testImplementation("io.kotest:kotest-assertions-core-jvm:4.2.0")
    testImplementation("io.kotest:kotest-assertions-json-jvm:4.3.1")
    testImplementation("io.kotest:kotest-property-jvm:4.2.0")
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

tasks.withType<Test> {
    useJUnitPlatform()
}
