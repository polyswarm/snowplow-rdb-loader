/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader

import cats.Monad
import cats.data.Validated._
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Cache, Control, FileSystem, Iglu, S3I, SQL, SSH}

// This project
import config.CliConfig
import loaders.Common.{ load, discover }

/**
 * Application entry point
 */
object Main {
  /**
   * If arguments or config is invalid exit with 1
   * and print errors to EMR stdout
   * If arguments and config are valid, but loading failed
   * print message to `track` bucket
   */
  def main(argv: Array[String]): Unit = {
    CliConfig.parse(argv) match {
      case Valid(config) =>
        val status = run(config)
        sys.exit(status)
      case Invalid(errors) =>
        println("Configuration error")
        errors.toList.foreach(error => println(error.message))
        sys.exit(1)
    }
  }

  /**
   * Initialize interpreter from parsed configuration and
   * run all IO actions through it. Should never throw exceptions
   *
   * @param config parsed configuration
   * @return exit code status. 0 for success, 1 if anything went wrong
   */
  def run(config: CliConfig): Int = {
    import cats.Id

    implicit val idCache: Cache[Id] = Cache.cacheInterpreter
    implicit val idControl: Control[Id] = Control.controlInterpreter
    implicit val idIglu: Iglu[Id] = Iglu.igluInterpreter
    implicit val idJdbc: SQL[Id] = if (config.dryRun) SQL.jdbcDryRunInterpreter else SQL.jdbcInterpreter
    implicit val idS3: S3I[Id] = S3I.s3Interpreter
    implicit val idSsh: SSH[Id] = SSH.sshInterpreter
    implicit val idFileSystem: FileSystem[Id] = FileSystem.fileSystemInterpreter

    val env: Environment[Id] = Environment.initialize[Id](config).right.get

    val data = discover[Id](env, config).flatTap(db.Migration.perform[Id](env, config.target.schema)).value
    val result = data match {
      case Right(discovery) => load[Id](env, config, discovery).value
      case Left(LoaderError.StorageTargetError(message)) =>
        val upadtedMessage = s"$message\n${env.getLastCopyStatements}"
        LoaderError.StorageTargetError(upadtedMessage).asLeft
      case Left(error) => error.asLeft
    }
    val message = utils.Common.interpret(config, result)
    Control.controlInterpreter.track(env, message)
    close[Id](env, config.logKey, message)
  }

  /** Get exit status based on all previous steps */
  private def close[F[_]: Monad: Control: S3I](env: Environment[F], logKey: Option[S3.Key], message: Log) = {
    logKey match {
      case Some(key) => for {
        dumpResult <- Control[F].dump(env, key)
        status     <- Control[F].exit(message, Some(dumpResult))
      } yield status
      case None => Control[F].exit(message, None)
    }
  }
}
