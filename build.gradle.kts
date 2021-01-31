import net.researchgate.release.GitAdapter
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.springframework.boot.gradle.tasks.run.BootRun

plugins {
    id("org.springframework.boot") version "2.4.2"
    id("io.spring.dependency-management") version "1.0.11.RELEASE"
    kotlin("jvm") version "1.4.21"
    kotlin("plugin.spring") version "1.4.21"
    id ("net.researchgate.release") version "2.8.1"
    application
    id( "com.github.ben-manes.versions") version "0.36.0"
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
    implementation("com.amazonaws:aws-java-sdk-core:1.11.946")
    implementation("com.amazonaws:aws-java-sdk-dynamodb:1.11.946")
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.946")
    implementation("com.amazonaws:aws-java-sdk-sns:1.11.946")
    implementation("com.amazonaws:aws-java-sdk-sqs:1.11.946")
    implementation("com.beust:klaxon:5.4")
    implementation("com.github.dwp:dataworks-common-logging:0.0.6")
    implementation("com.google.code.gson:gson:2.8.6")
    implementation("org.apache.commons:commons-compress:1.20")
    implementation("org.apache.commons:commons-text:1.9")
    implementation("org.apache.hbase:hbase-client:1.4.13")
    implementation("org.apache.httpcomponents:httpclient:4.5.13")
    implementation("org.bouncycastle:bcprov-ext-jdk15on:1.68")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-aop")
    implementation("org.springframework.boot:spring-boot-starter-batch")
    implementation("org.springframework.retry:spring-retry")

    testImplementation("junit:junit:4.13.1")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.batch:spring-batch-test")
    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.2.0")
    testImplementation("io.kotest:kotest-runner-junit5-jvm:4.3.2")
    testImplementation("io.kotest:kotest-assertions-core-jvm:4.3.2")
    testImplementation("io.kotest:kotest-assertions-json-jvm:4.3.2")
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
