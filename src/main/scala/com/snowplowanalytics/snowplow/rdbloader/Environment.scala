/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import java.sql.Connection

import com.jcraft.jsch.Session

import com.amazonaws.services.s3.AmazonS3

import cats.Monad

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.rdbloader.dsl.{ SQL, SSH, S3I }
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.discovery.ShreddedType.Json

import com.snowplowanalytics.snowplow.scalatracker.Tracker

sealed trait Environment[F[_]] {
  def config: CliConfig
  def connection: Connection
  def s3: AmazonS3
  def ssh: Option[Session]
  def igluClient: Client[F, Json]
  def tracker: Option[Tracker[F]]

  def getLastCopyStatements: String
}

object Environment {
  private case class LoaderEnvironment[F[_]](config: CliConfig,
                                             connection: Connection,
                                             s3: AmazonS3,
                                             ssh: Option[Session],
                                             igluClient: Client[F, Json],
                                             tracker: Option[Tracker[F]],
                                             log: List[String]
                                            ) extends Environment[F] {

    def getLastCopyStatements: String = log.headOption.toString
  }

  def initialize[F[_]: Monad: SSH](config: CliConfig): Either[String, Environment[F]] = {
    for {
      connection <- SQL.getConnection(config.target)
      s3 = S3I.getClient(config.configYaml.aws)
      session = SSH[F].establishTunnel(config.target.sshTunnel)

    } yield Environment.LoaderEnvironment(config, connection, s3)



    ???
  }

}
