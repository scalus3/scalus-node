import sbt.internal.util.ManagedLogger
import java.net.URI

// =============================================================================
// GLOBAL SETTINGS
// =============================================================================

Global / onChangedBuildSource := ReloadOnSourceChanges
autoCompilerPlugins := true

// Latest scalus snapshot — published to Sonatype Central Snapshots.
// Bump when a newer snapshot is needed.
val scalusVersion = "0.17.0+11-e1a753ab-SNAPSHOT"

val cardanoClientLibVersion = "0.7.1"
val yaciVersion = "0.4.0"
val yaciCardanoTestVersion = "0.1.0"
val scalatestVersion = "3.2.20"
val scalatestPlusScalacheckVersion = "3.2.19.0"
val slf4jVersion = "2.0.17"

ThisBuild / scalaVersion := "3.3.7"
ThisBuild / organization := "org.scalus"
ThisBuild / organizationName := "Scalus"
ThisBuild / organizationHomepage := Some(url("https://scalus.org/"))
ThisBuild / developers := List(
  Developer(
    id = "atlanter",
    name = "Alexander Nemish",
    email = "anemish@gmail.com",
    url = url("https://github.com/nau")
  )
)
ThisBuild / description := "Scalus embedded-node — streaming engine and Cardano network adapters"
ThisBuild / licenses := List(
  "Apache 2" -> new URI("http://www.apache.org/licenses/LICENSE-2.0.txt").toURL
)
ThisBuild / homepage := Some(url("https://github.com/scalus3/scalus-node"))
ThisBuild / versionScheme := Some("early-semver")
Test / publishArtifact := false

// Pull scalus snapshots from Sonatype Central Snapshots.
ThisBuild / resolvers += Resolver.sonatypeCentralSnapshots

ThisBuild / semanticdbEnabled := true

val javaVersion = sys.props("java.specification.version").toInt
ThisBuild / Test / javaOptions ++= (if (javaVersion >= 22) Seq("--enable-native-access=ALL-UNNAMED")
                                    else Nil)
ThisBuild / Test / javaOptions ++= (if (javaVersion >= 23)
                                        Seq("--sun-misc-unsafe-memory-access=allow")
                                    else Nil)
ThisBuild / run / javaOptions ++= (if (javaVersion >= 23)
                                       Seq("--sun-misc-unsafe-memory-access=allow")
                                   else Nil)

ThisBuild / incOptions := {
    incOptions.value
        .withLogRecompileOnMacro(false)
        .withUseOptimizedSealed(true)
}

ThisBuild / parallelExecution := true
Global / concurrentRestrictions := Seq(
  Tags.limitAll(java.lang.Runtime.getRuntime.availableProcessors())
)

lazy val commonScalacOptions = Seq(
  "-deprecation",
  "-feature",
  "-explain",
  "-Wunused:imports",
  "-Xcheck-macros"
)

// =============================================================================
// AGGREGATE
// =============================================================================

lazy val root: Project = project
    .in(file("."))
    .aggregate(
      scalusStreamingCore.jvm,
      scalusStreamingCore.js,
      scalusStreamingFs2.jvm,
      scalusStreamingFs2.js,
      scalusStreamingOx,
      scalusCardanoNetwork.jvm,
      scalusCardanoNetwork.js,
      scalusChainStoreMithril,
      scalusChainStoreRocksdb
    )
    .settings(
      name := "scalus-node",
      publish / skip := true,
    )

lazy val jvm: Project = project
    .in(file("jvm"))
    .aggregate(
      scalusStreamingCore.jvm,
      scalusStreamingFs2.jvm,
      scalusStreamingOx,
      scalusCardanoNetwork.jvm,
      scalusChainStoreMithril,
      scalusChainStoreRocksdb
    )
    .settings(
      publish / skip := true
    )

lazy val js: Project = project
    .in(file("js"))
    .aggregate(
      scalusStreamingCore.js,
      scalusStreamingFs2.js,
      scalusCardanoNetwork.js
    )
    .settings(
      publish / skip := true
    )

// =============================================================================
// PROJECTS
// =============================================================================

// Embedded-node streaming stack: core engine + flavor adapters.
// scalus-streaming-core holds the rollback-aware BlockchainStreamProvider
// engine, ADTs, and chain-sync adapters. Flavor modules add a concrete
// stream library (fs2 / ox) and the corresponding BlockchainStreamProvider
// subclass. Browser JS is not targeted — raw sockets are required.
lazy val scalusStreamingCore = crossProject(JSPlatform, JVMPlatform)
    .in(file("scalus-streaming-core"))
    .settings(
      name := "scalus-streaming-core",
      scalacOptions ++= commonScalacOptions,
      libraryDependencies += "org.scalus" %%% "scalus-cardano-ledger" % scalusVersion,
      libraryDependencies += "org.scalatest" %%% "scalatest" % scalatestVersion % "test",
      libraryDependencies += "org.scalatestplus" %%% "scalacheck-1-18" % scalatestPlusScalacheckVersion % "test",
      publish / skip := false
    )

// fs2 flavor: consumer-side fs2.Stream adapter + Fs2BlockchainStreamProvider
// + Fs2StreamingEmulator. Cross-built JVM+JS.
lazy val scalusStreamingFs2 = crossProject(JSPlatform, JVMPlatform)
    .in(file("scalus-streaming-fs2"))
    .dependsOn(
      scalusStreamingCore % "compile->compile;test->test",
      scalusCardanoNetwork % "compile->compile;test->test"
    )
    .settings(
      name := "scalus-streaming-fs2",
      scalacOptions ++= commonScalacOptions,
      libraryDependencies += "co.fs2" %%% "fs2-core" % "3.11.0",
      libraryDependencies += "org.scalatest" %%% "scalatest" % scalatestVersion % "test",
      libraryDependencies += "org.scalatestplus" %%% "scalacheck-1-18" % scalatestPlusScalacheckVersion % "test",
      publish / skip := false
    )

// ox flavor: consumer-side ox adapter + OxBlockchainStreamProvider +
// OxStreamingEmulator. JVM-only (ox itself is JVM-only).
lazy val scalusStreamingOx = project
    .in(file("scalus-streaming-ox"))
    .dependsOn(
      scalusStreamingCore.jvm % "compile->compile;test->test",
      scalusCardanoNetwork.jvm % "compile->compile;test->test"
    )
    .settings(
      name := "scalus-streaming-ox",
      scalacOptions ++= commonScalacOptions,
      libraryDependencies += "com.softwaremill.ox" %% "core" % "1.0.2",
      libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      publish / skip := false
    )

// Mithril-verified snapshot restore for ChainStore.
lazy val scalusChainStoreMithril = project
    .in(file("scalus-chain-store-mithril"))
    .dependsOn(
      scalusStreamingCore.jvm % "compile->compile;test->test",
      scalusChainStoreRocksdb % "test->compile"
    )
    .settings(
      name := "scalus-chain-store-mithril",
      scalacOptions ++= commonScalacOptions,
      libraryDependencies += "io.github.dotty-cps-async" %% "dotty-cps-async" % "1.3.2",
      libraryDependencies += "com.dylibso.chicory" % "runtime" % "1.7.0",
      libraryDependencies += "com.dylibso.chicory" % "wasi" % "1.7.0",
      libraryDependencies += "com.github.luben" % "zstd-jni" % "1.5.6-3",
      libraryDependencies += "org.apache.commons" % "commons-compress" % "1.28.0",
      libraryDependencies += "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.38.9",
      libraryDependencies += "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.38.9",
      // Blake2s256 — used by the Mithril Merkle tree (MMR) for cardano-database snapshot
      // verification. BouncyCastle's lightweight `Blake2sDigest` class is enough; we don't need
      // the JCA provider registration.
      libraryDependencies += "org.bouncycastle" % "bcprov-jdk18on" % "1.78",
      libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      publish / skip := false
    )

// RocksDB-backed ChainStore. JVM-only because rocksdbjni is a native library.
lazy val scalusChainStoreRocksdb = project
    .in(file("scalus-chain-store-rocksdb"))
    .dependsOn(scalusStreamingCore.jvm % "compile->compile;test->test")
    .settings(
      name := "scalus-chain-store-rocksdb",
      scalacOptions ++= commonScalacOptions,
      libraryDependencies += "org.rocksdb" % "rocksdbjni" % "9.7.3",
      libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      publish / skip := false
    )

// Ouroboros Cardano network protocols: Node-to-Node (N2N) transport.
// Depends on scalus-streaming-core for the Mailbox primitive.
lazy val scalusCardanoNetwork = crossProject(JSPlatform, JVMPlatform)
    .in(file("scalus-cardano-network"))
    .dependsOn(
      scalusStreamingCore % "compile->compile;test->test"
    )
    .settings(
      name := "scalus-cardano-network",
      scalacOptions ++= commonScalacOptions,
      libraryDependencies += "org.scalus" %%% "scalus-cardano-ledger" % scalusVersion,
      libraryDependencies += "io.github.dotty-cps-async" %%% "dotty-cps-async" % "1.3.2",
      libraryDependencies += "org.scalatest" %%% "scalatest" % scalatestVersion % "test",
      libraryDependencies += "org.scalatestplus" %%% "scalacheck-1-18" % scalatestPlusScalacheckVersion % "test",
      publish / skip := false
    )

// Cardano network integration tests — spins up yaci-devkit via testcontainers
// and exercises the real SDU multiplexer, handshake, and keep-alive over a
// real TCP connection. Preview-relay smoke tests live here too, gated by
// SCALUS_N2N_PREVIEW_IT=1.
lazy val scalusCardanoNetworkIt = project
    .in(file("scalus-cardano-network-it"))
    .dependsOn(
      scalusCardanoNetwork.jvm % "compile->compile;test->test",
      scalusStreamingFs2.jvm % "test->compile",
      scalusStreamingOx % "test->compile"
    )
    .settings(
      name := "scalus-cardano-network-it",
      scalacOptions ++= commonScalacOptions,
      publish / skip := true,
      Test / fork := true,
      Test / testOptions += Tests.Argument("-oF"),
      libraryDependencies += "org.scalus" %% "scalus-testkit" % scalusVersion % "test",
      libraryDependencies += "com.bloxbean.cardano" % "yaci" % yaciVersion % "test",
      libraryDependencies += "com.bloxbean.cardano" % "yaci-cardano-test" % yaciCardanoTestVersion % "test",
      libraryDependencies += "com.dimafeng" %% "testcontainers-scala-core" % "0.44.1" % "test",
      libraryDependencies += "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.44.1" % "test",
      libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      libraryDependencies += "org.slf4j" % "slf4j-simple" % slf4jVersion % "test"
    )

// =============================================================================
// COMMAND ALIASES
// =============================================================================

addCommandAlias(
  "compileAll",
  "scalafmtAll;scalafmtSbt;jvm/Test/compile;scalusCardanoNetworkIt/Test/compile;jvm/testQuick"
)
addCommandAlias(
  "testAll",
  "clean;scalafmtAll;scalafmtSbt;jvm/Test/compile;scalusCardanoNetworkIt/Test/compile;jvm/test"
)
