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

import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList

import com.snowplowanalytics.snowplow.rdbloader.Environment
import com.snowplowanalytics.snowplow.rdbloader.common._
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure
import com.snowplowanalytics.snowplow.rdbloader.LoaderError


trait Iglu[F[_]] {
  /** Retrieve list of schemas from Iglu Server */
  def getSchemas(env: Environment[F], vendor: String, name: String, model: Int): F[Either[LoaderError, SchemaList]]
}

object Iglu {
  def apply[F[_]](implicit ev: Iglu[F]): Iglu[F] = ev

  val igluInterpreter: Iglu[Id] = new Iglu[Id] {
    def getSchemas(env: Environment[Id], vendor: String, name: String, model: Int): Id[Either[LoaderError, SchemaList]] =
      Flattening.getOrdered(env.igluClient.resolver, vendor, name, model).leftMap { resolutionError =>
        val message = s"Cannot get schemas for iglu:$vendor/$name/jsonschema/$model-*-*\n$resolutionError"
        LoaderError.DiscoveryError(DiscoveryFailure.IgluError(message))
      }.value
  }
}
