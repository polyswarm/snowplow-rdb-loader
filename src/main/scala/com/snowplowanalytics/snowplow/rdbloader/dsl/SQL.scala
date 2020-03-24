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

import java.io.FileReader
import java.nio.file.Path
import java.sql.{Connection, SQLException}
import java.util.Properties

import scala.util.control.NonFatal

import cats.{Id, Monad}
import cats.data.EitherT
import cats.implicits._

import org.postgresql.copy.CopyManager
import org.postgresql.jdbc.PgConnection
import org.postgresql.{Driver => PgDriver}

import com.amazon.redshift.jdbc42.{Driver => RedshiftDriver}

import com.snowplowanalytics.snowplow.rdbloader.{Environment, LoaderAction, LoaderError}
import com.snowplowanalytics.snowplow.rdbloader.LoaderError.StorageTargetError
import com.snowplowanalytics.snowplow.rdbloader.common.StorageTarget
import com.snowplowanalytics.snowplow.rdbloader.db.Decoder
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString

trait SQL[F[_]] {

  /** Execute single SQL statement (against target in interpreter) */
  def executeUpdate(env: Environment[F], sql: SqlString): LoaderAction[F, Long]

  /** Execute multiple (against target in interpreter) */
  def executeUpdates(env: Environment[F], queries: List[SqlString])(implicit A: Monad[F]): LoaderAction[F, Unit] =
    EitherT(queries.traverse_(executeUpdate(env, _)).value)

  /** Execute query and parse results into `A` */
  def executeQuery[A](env: Environment[F], query: SqlString)(implicit ev: Decoder[A]): LoaderAction[F, A]

  /** Perform PostgreSQL COPY table FROM STDIN (against target in interpreter) */
  def copyViaStdin(env: Environment[F], files: List[Path], query: SqlString): LoaderAction[F, Long]

  /** Execute SQL transaction (against target in interpreter) */
  def executeTransaction(env: Environment[F], queries: List[SqlString])(implicit A: Monad[F]): LoaderAction[F, Unit] = {
    val begin = SqlString.unsafeCoerce("BEGIN")
    val commit = SqlString.unsafeCoerce("COMMIT")
    val transaction = (begin :: queries) :+ commit
    executeUpdates(env, transaction)
  }
}

object SQL {
  def apply[F[_]](implicit ev: SQL[F]): SQL[F] = ev

  def getConnection[F[_]: Monad: SSH](target: StorageTarget): LoaderAction[F, Connection] = {
    val password: LoaderAction[F, String] = target.password match {
      case StorageTarget.PlainText(text) =>
        LoaderAction.lift(text)
      case StorageTarget.EncryptedKey(StorageTarget.EncryptedConfig(key)) =>
        LoaderAction(SSH[F].getEc2Property(key.parameterName))
    }

    def connect(props: Properties) =
      Either.catchNonFatal(new RedshiftDriver().connect(s"jdbc:redshift://${target.host}:${target.port}/${target.database}", props))

    for {
      p <- password
      props = new Properties()
      _ = props.setProperty("user", target.username)
      _ = props.setProperty("password", p)

      result = target match {
        case r: StorageTarget.RedshiftConfig =>

          for {
            _ <- r.jdbc.validation match {
              case Left(error) => LoaderError.ConfigError(error.message).asLeft
              case Right(propertyUpdaters) => propertyUpdaters.foreach(f => f(props)).asRight
            }
            firstAttempt = connect(props)
            connection <- firstAttempt match {
              case Right(c) =>
                c.asRight
              case Left(e) =>
                println("Error during connection acquisition. Sleeping and making another attempt")
                e.printStackTrace(System.out)
                Thread.sleep(60000)
                connect(props).leftMap(e2 => LoaderError.StorageTargetError(e2.getMessage))
            }
          } yield connection

        case p: StorageTarget.PostgresqlConfig =>
          val url = s"jdbc:postgresql://${p.host}:${p.port}/${p.database}"
          props.setProperty("sslmode", p.sslMode.asProperty)
          Right(new PgDriver().connect(url, props))
      }
      c <- LoaderAction.liftE[F, Connection](result)
    } yield c
  }

  val jdbcInterpreter: SQL[Id] = new SQL[Id] {
    /**
     * Execute a single update-statement in provided Postgres connection
     *
     * @param sql string with valid SQL statement
     * @return number of updated rows in case of success, failure otherwise
     */
    def executeUpdate(env: Environment[Id], sql: SqlString): LoaderAction[Id, Long] = {
      val result = Either.catchNonFatal {
        env.connection.createStatement().executeUpdate(sql).toLong
      } leftMap {
        case NonFatal(e: java.sql.SQLException) if Option(e.getMessage).getOrElse("").contains("is not authorized to assume IAM Role") =>
          StorageTargetError("IAM Role with S3 Read permissions is not attached to Redshift instance")
        case NonFatal(e) =>
          println("RDB Loader unknown error in executeUpdate")
          e.printStackTrace(System.out)
          StorageTargetError(Option(e.getMessage).getOrElse(e.toString))
      }

      LoaderAction.liftE(result)
    }

    def executeQuery[A](env: Environment[Id], sql: SqlString)(implicit ev: Decoder[A]): LoaderAction[Id, A] = {
      val result = try {
        val resultSet = env.connection.createStatement().executeQuery(sql)
        ev.decode(resultSet) match {
          case Left(e) => StorageTargetError(s"Cannot decode SQL row: ${e.message}").asLeft
          case Right(a) => a.asRight[StorageTargetError]
        }
      } catch {
        case NonFatal(e) =>
          println("RDB Loader unknown error in executeQuery")
          e.printStackTrace(System.out)
          StorageTargetError(Option(e.getMessage).getOrElse(e.toString)).asLeft[A]
      }

      LoaderAction.liftE(result)
    }

    def copyViaStdin(env: Environment[Id], files: List[Path], copyStatement: SqlString): LoaderAction[Id, Long] = {
      val conn: Connection = env.connection

      val copyManager = Either.catchNonFatal {
        new CopyManager(conn.asInstanceOf[PgConnection])
      } leftMap { e => StorageTargetError(e.toString) }

      val result = for {
        manager <- copyManager
        _ <- setAutocommit(conn, false)
        result = files.traverse(copyIn(manager, copyStatement)(_)).map(_.combineAll)
        _ = result.fold(_ => conn.rollback(), _ => conn.commit())
        _ <- setAutocommit(conn, true)
        endResult <- result
      } yield endResult

      LoaderAction.liftE(result)
    }

    def copyIn(copyManager: CopyManager, copyStatement: String)(file: Path): Either[LoaderError, Long] =
      try {
        Right(copyManager.copyIn(copyStatement, new FileReader(file.toFile)))
      } catch {
        case NonFatal(e) => Left(StorageTargetError(e.toString))
      }

    def setAutocommit(conn: Connection, autoCommit: Boolean): Either[LoaderError, Unit] =
      try {
        Right(conn.setAutoCommit(autoCommit))
      } catch {
        case e: SQLException =>
          println("setAutocommit error")
          e.printStackTrace(System.out)
          Left(StorageTargetError(e.toString))
      }
  }

  val jdbcDryRunInterpreter: SQL[Id] = new SQL[Id] {
    def executeUpdate(env: Environment[Id], sql: SqlString): LoaderAction[Id, Long] = {
      val conn: Connection = env.connection

      val result = Either.catchNonFatal {
        conn.createStatement().executeUpdate(sql).toLong
      } leftMap {
        case NonFatal(e: java.sql.SQLException) if Option(e.getMessage).getOrElse("").contains("is not authorized to assume IAM Role") =>
          StorageTargetError("IAM Role with S3 Read permissions is not attached to Redshift instance")
        case NonFatal(e) =>
          println("RDB Loader unknown error in executeUpdate")
          e.printStackTrace(System.out)
          StorageTargetError(Option(e.getMessage).getOrElse(e.toString))
      }

      LoaderAction.liftE(result)
    }

    def executeQuery[A](env: Environment[Id], sql: SqlString)(implicit ev: Decoder[A]): LoaderAction[Id, A] = {
      val conn: Connection = env.connection

      val result = try {
        val resultSet = conn.createStatement().executeQuery(sql)
        ev.decode(resultSet) match {
          case Left(e) => StorageTargetError(s"Cannot decode SQL row: ${e.message}").asLeft
          case Right(a) => a.asRight[StorageTargetError]
        }
      } catch {
        case NonFatal(e) =>
          println("RDB Loader unknown error in executeQuery")
          e.printStackTrace(System.out)
          StorageTargetError(Option(e.getMessage).getOrElse(e.toString)).asLeft[A]
      }

      LoaderAction.liftE(result)
    }

    def copyViaStdin(env: Environment[Id], files: List[Path], copyStatement: SqlString): LoaderAction[Id, Long] = {
      val conn: Connection = env.connection

      val copyManager = Either.catchNonFatal {
        new CopyManager(conn.asInstanceOf[PgConnection])
      } leftMap { e => StorageTargetError(e.toString) }

      val result = for {
        manager <- copyManager
        result = files.traverse(copyIn(manager, copyStatement)(_)).map(_.combineAll)
        _ = result.fold(_ => conn.rollback(), _ => conn.commit())
        endResult <- result
      } yield endResult

      LoaderAction.liftE(result)
    }

    def copyIn(copyManager: CopyManager, copyStatement: String)(file: Path): Either[LoaderError, Long] =
      try {
        Right(copyManager.copyIn(copyStatement, new FileReader(file.toFile)))
      } catch {
        case NonFatal(e) => Left(StorageTargetError(e.toString))
      }
  }
}

