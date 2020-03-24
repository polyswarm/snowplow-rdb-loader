/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
package db

import cats.{Functor, Monad}
import cats.data.EitherT
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap, SchemaVer}
import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, Migration => DMigration, SchemaList => DSchemaList}
import com.snowplowanalytics.iglu.schemaddl.redshift.Ddl
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.{DdlGenerator, MigrationGenerator}

import com.snowplowanalytics.snowplow.rdbloader.db.Entities.{Columns, TableState}
import com.snowplowanalytics.snowplow.rdbloader.discovery.{DataDiscovery, DiscoveryFailure, ShreddedType}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Control, Iglu, SQL}
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString

object Migration {
  /**
    * Perform all the machinery to check if any tables for tabular data do not match
    * latest state on the Iglu Server. Create or update tables in that case.
    * Do nothing in case there's only legacy JSON data
    */
  def perform[F[_]: Monad: Control: Iglu: SQL](env: Environment[F], dbSchema: String)(discoveries: List[DataDiscovery]): LoaderAction[F, Unit] =
    discoveries.flatMap(_.shreddedTypes).traverse_ {
      case ShreddedType.Tabular(ShreddedType.Info(_, vendor, name, model, _)) =>
        for {
          schemas   <- EitherT(Iglu[F].getSchemas(env, vendor, name, model))
          tableName  = StringUtils.getTableName(SchemaMap(SchemaKey(vendor, name, "jsonschema", SchemaVer.Full(model, 0, 0))))
          _         <- for {
            exists  <- tableExists[F](env, dbSchema, tableName)
            _       <- if (exists) for {
              description <- getVersion[F](env, dbSchema, tableName, schemas)
              matches      = schemas.latest.schemaKey == description.version
              columns     <- getColumns[F](env, dbSchema, tableName)
              _           <- if (matches) LoaderAction.unit[F] else updateTable[F](env, dbSchema, description.version, columns, schemas)
            } yield () else createTable[F](env, dbSchema, tableName, schemas)
          } yield ()
        } yield ()
      case ShreddedType.Json(_, _) => LoaderAction.unit[F]
    }

  /** Find the latest schema version in the table and confirm that it is the latest in `schemas` */
  def getVersion[F[_]: Monad: SQL](env: Environment[F], dbSchema: String, tableName: String, latest: DSchemaList): LoaderAction[F, TableState] = {
    val query = SqlString.unsafeCoerce(s"SELECT obj_description(oid) FROM pg_class WHERE relname = '$tableName'")
    SQL[F].executeUpdate(env, SqlString.unsafeCoerce(s"SET SEARCH_PATH TO $dbSchema")) *>
      SQL[F].executeQuery[TableState](env, query).leftMap(annotateError(dbSchema, tableName))
  }

  /** Check if table exists in `dbSchema` */
  def tableExists[F[_]: Functor: SQL](env: Environment[F], dbSchema: String, table: String): LoaderAction[F, Boolean] = {
    val query = SqlString.unsafeCoerce(
      s"""
         |SELECT EXISTS (
         |   SELECT 1
         |   FROM   pg_tables
         |   WHERE  schemaname = '$dbSchema'
         |   AND    tablename = '$table') AS exists;
      """.stripMargin)

    SQL[F].executeQuery[Boolean](env, query).leftMap(annotateError(dbSchema, table))
  }

  def createTable[F[_]: Monad: Control: SQL](env: Environment[F], dbSchema: String, name: String, schemas: DSchemaList): LoaderAction[F, Unit] = {
    val subschemas = FlatSchema.extractProperties(schemas)
    val tableName = StringUtils.getTableName(schemas.latest)
    val ddl = DdlGenerator.generateTableDdl(subschemas, tableName, Some(dbSchema), 4096, false)
    val comment = DdlGenerator.getTableComment(name, Some(dbSchema), schemas.latest)
    Control[F].print(s"Creating $dbSchema.$name table for ${comment.comment}").liftA *>
      SQL[F].executeUpdate(env, ddl.toSql).void *>
      SQL[F].executeUpdate(env, comment.toSql).void *>
      Control[F].print(s"Table created").liftA
  }

  /** Update existing table specified by `current` into a final version present in `state` */
  def updateTable[F[_]: Monad: SQL: Control](env: Environment[F], dbSchema: String, current: SchemaKey, columns: Columns, state: DSchemaList): LoaderAction[F, Unit] =
    state match {
      case s: DSchemaList.Full =>
        val migrations = s.extractSegments.map(DMigration.fromSegment)
        migrations.find(_.from == current.version) match {
          case Some(relevantMigration) =>
            val ddlFile = MigrationGenerator.generateMigration(relevantMigration, 4096, Some(dbSchema))
            val ddl = SqlString.unsafeCoerce(ddlFile.render)
            LoaderAction.liftA(ddlFile.warnings.traverse_(Control[F].print)) *>
              LoaderAction.liftA(Control[F].print(s"Executing migration DDL statement: $ddl")) *>
              SQL[F].executeUpdate(env, ddl).void
          case None =>
            val message = s"Warning: Table's schema key '${current.toSchemaUri}' cannot be found in fetched schemas $state. Migration cannot be created"
            LoaderAction.liftE[F, Unit](DiscoveryFailure.IgluError(message).toLoaderError.asLeft)
        }
      case _: DSchemaList.Single =>
        Control[F].print(s"Warning: updateTable executed for a table with single schema\ncolumns: $columns\nstate: $state").liftA
    }

  /** List all columns in the table */
  def getColumns[F[_]: Monad: SQL](env: Environment[F], dbSchema: String, tableName: String): LoaderAction[F, Columns] = {
    val setSchema = SqlString.unsafeCoerce(s"SET search_path TO $dbSchema;")
    val getColumns = SqlString.unsafeCoerce(s"""SELECT "column" FROM PG_TABLE_DEF WHERE tablename = '$tableName';""")
    for {
      _       <- SQL[F].executeUpdate(env, setSchema)
      columns <- SQL[F].executeQuery[Columns](env, getColumns).leftMap(annotateError(dbSchema, tableName))
    } yield columns
  }

  private def annotateError(dbSchema: String, tableName: String)(error: LoaderError): LoaderError =
    error match {
      case LoaderError.StorageTargetError(message) =>
        LoaderError.StorageTargetError(s"$dbSchema.$tableName. " ++ message)
      case other =>
        other
    }

  private implicit class SqlDdl(ddl: Ddl) {
    def toSql: SqlString =
      SqlString.unsafeCoerce(ddl.toDdl)
  }
}
