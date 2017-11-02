name		:= "mptcp-analysis"
version		:= "1.0"
organization	:= "ikt"
scalaVersion	:= "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "2.1.1"
// libraryDependencies += "org.jfree" % "jfreechart" % "1.0.14"
// libraryDependencies += "com.quantifind" %% "wisp" % "0.0.4"
//libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"
//libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.13.2"
// libraryDependencies += "org.scalanlp" %% "breeze-viz" % "0.13.2"
// libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.1"
//libraryDependencies += "org.vegas-viz" % "vegas_2.11" % "0.3.11"
libraryDependencies += "org.vegas-viz" % "vegas-spark_2.11" % "0.3.11"
libraryDependencies += "org.vegas-viz" % "vegas-macros_2.11" % "0.3.11"


resolvers	+= Resolver.mavenLocal
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
// fork in run := true

