import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.testing.logging.TestLogEvent.*
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.tasks.Copy

plugins {
  java
  application
  id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.agents.director"
version = "1.0.0"

repositories {
  mavenCentral()
  maven {
    url = uri("https://oss.sonatype.org/content/repositories/snapshots")
  }
}

val junitJupiterVersion = "5.9.1"
val vertxVersion = "4.5.7"
val awsSDKVersion = "2.25.31"
val mcpVersion = "0.11.0"
val reactorVersion = "3.6.0"
val launcherClassName = "agents.director.Driver"

application {
  mainClass.set(launcherClassName)
}

dependencies {
  // MCP SDK dependencies
  implementation("io.modelcontextprotocol.sdk:mcp:$mcpVersion")
  
  // Reactor dependencies for MCP SDK
  implementation("io.projectreactor:reactor-core:$reactorVersion")
  
  // Vert.x dependencies
  implementation(platform("io.vertx:vertx-stack-depchain:$vertxVersion"))
  implementation("io.vertx:vertx-web-client")
  implementation("io.vertx:vertx-config")
  implementation("io.vertx:vertx-auth-jwt")
  implementation("io.vertx:vertx-web")
  implementation("io.vertx:vertx-health-check")
  implementation(platform("software.amazon.awssdk:bom:$awsSDKVersion"))
  implementation("software.amazon.awssdk:dynamodb")
  implementation("software.amazon.awssdk:ec2")
  implementation("software.amazon.awssdk:lambda")
  implementation("software.amazon.awssdk:cloudwatch")
  implementation("software.amazon.awssdk:route53")
  implementation("software.amazon.awssdk:s3")
  implementation("software.amazon.awssdk:ssm")
  implementation("software.amazon.awssdk:netty-nio-client")
  testImplementation("io.vertx:vertx-junit5")
  testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
  testImplementation("org.mockito:mockito-core:5.11.0")
  testImplementation("org.mockito:mockito-junit-jupiter:5.11.0")
  implementation("com.fasterxml.jackson.core:jackson-core:2.15.3")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.15.3")
  implementation("com.fasterxml.jackson.core:jackson-annotations:2.15.3")
  implementation("com.vladsch.flexmark:flexmark-all:0.64.8")
  
  // Oracle JDBC dependencies
  implementation("com.oracle.database.jdbc:ojdbc11:21.11.0.0")
  implementation("com.oracle.database.jdbc:ucp:21.11.0.0")
  
  // SQL parsing and manipulation
  implementation("com.github.jsqlparser:jsqlparser:4.7")
  
  // Natural language processing
  implementation("org.apache.opennlp:opennlp-tools:2.3.1")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-Xlint:unchecked")
}

tasks.withType<ShadowJar> {
    archiveClassifier.set("fat")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    mergeServiceFiles()
}

tasks.withType<Test> {
  useJUnitPlatform()
  testLogging {
    events = setOf(PASSED, SKIPPED, FAILED, STANDARD_OUT, STANDARD_ERROR)
    exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    showStackTraces = true
    showCauses = true
  }
}

tasks.withType<Copy>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// Configure the run task explicitly
tasks.named<JavaExec>("run") {
    // Ensure clean build before running to avoid file locks
    dependsOn("classes")
    
    // Set main class explicitly
    mainClass.set(launcherClassName)
    
    // Add JVM arguments for better Windows compatibility
    jvmArgs = listOf(
        "-Xmx2g",
        "-Dfile.encoding=UTF-8",
        "-Djava.awt.headless=true"
    )
    
    // Set working directory
    workingDir = projectDir
    
    // Ensure classpath is set correctly
    classpath = sourceSets["main"].runtimeClasspath
}

// Add a clean task that handles Windows file locks better
tasks.named("clean") {
    doFirst {
        // Try to delete build directory, ignore errors on Windows
        delete(layout.buildDirectory)
    }
}
