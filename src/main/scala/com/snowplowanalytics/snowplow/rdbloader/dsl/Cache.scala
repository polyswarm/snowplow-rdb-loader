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

import cats.Id

import com.snowplowanalytics.snowplow.rdbloader.S3

trait Cache[F[_]] {
  /** Put value into cache (stored in interpreter) */
  def putCache(key: String, value: Option[S3.Key]): F[Unit]

  /** Get value from cache (stored in interpreter) */
  def getCache(key: String): F[Option[Option[S3.Key]]]
}

object Cache {
  def apply[F[_]](implicit ev: Cache[F]): Cache[F] = ev

  val cacheInterpreter: Cache[Id] = new Cache[Id] {
    private val cache = collection.mutable.HashMap.empty[String, Option[S3.Key]]

    def getCache(key: String): Id[Option[Option[S3.Key]]] =
      cache.get(key)

    def putCache(key: String, value: Option[S3.Key]): Id[Unit] =
      cache.put(key, value)
  }
}

