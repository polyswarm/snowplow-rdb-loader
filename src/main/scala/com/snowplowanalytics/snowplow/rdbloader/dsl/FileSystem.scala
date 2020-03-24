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

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

import scala.util.control.NonFatal

import cats.Id
import cats.data.EitherT
import cats.instances.either._
import cats.syntax.either._
import cats.syntax.functor._

import com.snowplowanalytics.snowplow.rdbloader.{ LoaderAction, LoaderError }

trait FileSystem[F[_]] {
  /** Create tmp directory */
  def createTmpDir: LoaderAction[F, Path]

  /** Delete directory */
  def deleteDir(path: Path): LoaderAction[F, Unit]
}

object FileSystem {
  def apply[F[_]](implicit ev: FileSystem[F]): FileSystem[F] = ev

  object DeleteVisitor extends SimpleFileVisitor[Path] {
    override def visitFile(file: Path, attrs: BasicFileAttributes) = {
      Files.delete(file)
      FileVisitResult.CONTINUE
    }
  }

  val fileSystemInterpreter: FileSystem[Id] = new FileSystem[Id] {
    def createTmpDir: LoaderAction[Id, Path] =
      EitherT.fromEither[Id](
        try {
          Files.createTempDirectory("rdb-loader").asRight
        } catch {
          case NonFatal(e) =>
            LoaderError.LoaderLocalError("Cannot create temporary directory.\n" + e.toString).asLeft
        })

    def deleteDir(path: Path): LoaderAction[Id, Unit] =
      EitherT.fromEither[Id](
        try {
          Files.walkFileTree(path, DeleteVisitor).asRight[LoaderError].void
        } catch {
          case NonFatal(e) => LoaderError.LoaderLocalError(s"Cannot delete directory [${path.toString}].\n" + e.toString).asLeft
        })
  }
}

