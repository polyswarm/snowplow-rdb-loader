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
package com.snowplowanalytics.snowplow.rdbloader.dsl

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import org.joda.time.DateTime

import scala.util.control.NonFatal

import cats.Id
import cats.data.NonEmptyList

import io.circe.Json

import com.amazonaws.services.s3.model.ObjectMetadata

import com.snowplowanalytics.iglu.core.{ SelfDescribingData, SchemaKey, SchemaVer }

import com.snowplowanalytics.snowplow.scalatracker.{ Tracker, Emitter }
import com.snowplowanalytics.snowplow.scalatracker.emitters.id.RequestProcessor._
import com.snowplowanalytics.snowplow.scalatracker.emitters.id.{SyncBatchEmitter, SyncEmitter}

import com.snowplowanalytics.snowplow.rdbloader.{Log, S3, Environment}
import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.config.CliConfig
import com.snowplowanalytics.snowplow.rdbloader.config.SnowplowConfig.{GetMethod, Monitoring, PostMethod}
import com.snowplowanalytics.snowplow.rdbloader.utils.Common


trait Control[F[_]] {

  /** Block thread for some time, milliseconds */
  def sleep(timeout: Long): F[Unit]

  /** Track result via Snowplow tracker */
  def track(env: Environment[F], result: Log): F[Unit]

  /** Dump log to S3 */
  def dump(env: Environment[F], key: S3.Key)(implicit S: S3I[F]): F[Either[String, S3.Key]]

  /** Close RDB Loader app with appropriate state */
  def exit(result: Log, dumpResult: Option[Either[String, S3.Key]]): F[Int]

  /** Print message to stdout */
  def print(message: String): F[Unit]
}

object Control {
  def apply[F[_]](implicit ev: Control[F]): Control[F] = ev

  val ApplicationContextSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "application_context", "jsonschema", SchemaVer.Full(1,0,0))
  val LoadSucceededSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_succeeded", "jsonschema", SchemaVer.Full(1,0,0))
  val LoadFailedSchema = SchemaKey("com.snowplowanalytics.monitoring.batch", "load_failed", "jsonschema", SchemaVer.Full(1,0,0))

  val controlInterpreter: Control[Id] = new Control[Id] {

    def log(s: String) = ???

    /** Block thread for some time, milliseconds */
    def sleep(timeout: Long): Id[Unit] = {
      log(s"Sleeping $timeout milliseconds")
      Thread.sleep(timeout)
    }

    /** Track result via Snowplow tracker */
    def track(env: Environment[Id], result: Log): Id[Unit] = {
      result match {
        case Log.LoadingSucceeded =>
          env.tracker match {
            case Some(t) =>
              t.trackSelfDescribingEvent(SelfDescribingData(LoadSucceededSchema, Json.fromFields(List.empty)))
            case None => ()
          }
          log(result.toString)
        case Log.LoadingFailed(message) =>
          val secrets = List(env.config.target.password.getUnencrypted, env.config.target.username)
          val sanitizedMessage = Common.sanitize(message, secrets)
          env.tracker match {
            case Some(t) =>
              t.trackSelfDescribingEvent(SelfDescribingData(LoadFailedSchema, Json.fromFields(List.empty)))
            case None => ()
          }
          log(sanitizedMessage)
      }
    }

    /** Dump log to S3 */
    def dump(env: Environment[Id], key: S3.Key)(implicit S: S3I[Id]): Id[Either[String, S3.Key]] = {
      // General messages that should be printed both to output and final log
      val messages = collection.mutable.ListBuffer.empty[String]

      log(s"Dumping $key")
      val logs = messages.mkString("\n") + "\n"

      try {
        if (S.keyExists(env, key)) {
          Left(s"S3 log object [$key] already exists")
        } else {
          val meta = new ObjectMetadata()
          meta.setContentLength(logs.length)
          meta.setContentEncoding("text/plain")

          val (bucket, prefix) = S3.splitS3Key(key)
          val is = new ByteArrayInputStream(logs.getBytes(StandardCharsets.UTF_8))
          env.s3.putObject(bucket, prefix, is, meta)
          Right(key)
        }
      } catch {
        case NonFatal(e) =>
          Left(e.toString)
      }
    }

    /** Close RDB Loader app with appropriate state */
    def exit(result: Log, dumpResult: Option[Either[String, S3.Key]]): Id[Int] =
      (result, dumpResult) match {
        case (Log.LoadingSucceeded, None) =>
          println(s"INFO: Logs were not dumped to S3")
          0
        case (Log.LoadingFailed(_), None) =>
          println(s"INFO: Logs were not dumped to S3")
          1
        case (Log.LoadingSucceeded, Some(Right(key))) =>
          println(s"INFO: Logs successfully dumped to S3 [$key]")
          0
        case (Log.LoadingFailed(_), Some(Right(key))) =>
          println(s"INFO: Logs successfully dumped to S3 [$key]")
          1
        case (_, Some(Left(error))) =>
          println(s"ERROR: Log-dumping failed: [$error]")
          1
      }

    /** Print message to stdout */
    def print(message: String): Id[Unit] =
      log(message)
  }

  /**
   * Initialize Snowplow tracker, if `monitoring` section is properly configured
   *
   * @param monitoring config.yml `monitoring` section
   * @return some tracker if enabled, none otherwise
   */
  def initializeTracking(monitoring: Monitoring): Option[Tracker[Id]] = {
    monitoring.snowplow.flatMap(_.collector) match {
      case Some(Collector((host, port))) =>
        val emitter: Emitter[Id] = monitoring.snowplow.flatMap(_.method) match {
          case Some(GetMethod) =>
            SyncEmitter.createAndStart(host, port = Some(port), callback = Some(callback))
          case Some(PostMethod) =>
            SyncBatchEmitter.createAndStart(host, port = Some(port), bufferSize = 2)
          case None =>
            SyncEmitter.createAndStart(host, port = Some(port), callback = Some(callback))
        }
        val tracker = new Tracker[Id](NonEmptyList.of(emitter), "snowplow-rdb-loader", monitoring.snowplow.flatMap(_.appId).getOrElse("rdb-loader"))
        Some(tracker)
      case Some(_) => None
      case None => None
    }
  }

  /** Callback for failed  */
  private def callback(params: CollectorParams, request: CollectorRequest, response: CollectorResponse): Unit = {
    def toMsg(rsp: CollectorResponse, includeHeader: Boolean): String = rsp match {
      case CollectorFailure(code) =>
        val header = if (includeHeader) { s"Snowplow Tracker [${DateTime.now()}]: " } else ""
        header ++ s"Cannot deliver event to ${params.getUri}. Collector responded with $code"
      case TrackerFailure(error) =>
        val header = if (includeHeader) { s"Snowplow Tracker [${DateTime.now()}]: " } else ""
        header ++ s"Cannot deliver event to ${params.getUri}. Tracker failed due ${error.getMessage}"
      case RetriesExceeded(r) => s"Tracker [${DateTime.now()}]: Gave up on trying to deliver event. Last error: ${toMsg(r, false)}"
      case CollectorSuccess(_) => ""
    }

    val message = toMsg(response, true)

    // The only place in interpreters where println used instead of logger as this is async function
    if (message.isEmpty) () else println(message)
  }


  /**
   * Config helper functions
   */
  private object Collector {
    def isInt(s: String): Boolean = try { s.toInt; true } catch { case _: NumberFormatException => false }

    def unapply(hostPort: String): Option[(String, Int)] =
      hostPort.split(":").toList match {
        case host :: port :: Nil if isInt(port) => Some((host, port.toInt))
        case host :: Nil => Some((host, 80))
        case _ => None
      }
  }
}

