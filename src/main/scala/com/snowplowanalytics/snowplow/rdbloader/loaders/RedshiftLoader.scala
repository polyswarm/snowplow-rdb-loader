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
package loaders

import cats.Monad
import cats.implicits._
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Control, SQL}

// This project
import common.StorageTarget

import RedshiftLoadStatements._
import Common.{ SqlString, EventsTable, checkLoadManifest, AtomicEvents, TransitTable }
import discovery.DataDiscovery
import config.{ SnowplowConfig, Step }


/**
 * Module containing specific for Redshift target loading
 * Works in three steps:
 * 1. Discover all data in shredded.good
 * 2. Construct SQL-statements
 * 3. Load data into Redshift
 * Errors of discovering steps are accumulating
 */
object RedshiftLoader {

  /**
   * Build `LoaderA` structure to discovery data in `shredded.good`
   * and associated metadata (types, JSONPaths etc),
   * build SQL statements to load this data and perform loading.
   * Primary working method. Does not produce side-effects
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   */
  def run[F[_]: Monad: Control: SQL](env: Environment[F],
                                     config: SnowplowConfig,
                                     target: StorageTarget.RedshiftConfig,
                                     steps: Set[Step],
                                     discovery: List[DataDiscovery]) =
    buildQueue(config, target, steps)(discovery).traverse_(loadFolder[F](env, steps))

  /**
   * Perform data-loading for a single run folder.
   *
   * @param statements prepared load statements
   * @return application state
   */
  def loadFolder[F[_]: Monad: Control: SQL](env: Environment[F], steps: Set[Step])(statements: RedshiftLoadStatements): LoaderAction[F, Unit] = {
    val checkManifest = steps.contains(Step.LoadManifestCheck)
    val loadManifest = steps.contains(Step.LoadManifest)

    def loadTransaction = for {
      empty <- getLoad[F](env, checkManifest, statements.dbSchema, statements.atomicCopy, statements.discovery.possiblyEmpty)
      _ <- SQL[F].executeUpdates(env, statements.shredded)
      _ <- if (loadManifest && !empty) SQL[F].executeUpdate(env, statements.manifest) *> Control[F].print("Load manifest: added new record").liftA
           else if (loadManifest && empty) Control[F].print(EmptyMessage).liftA
           else LoaderAction.unit[F]
    } yield ()

    for {
      _ <- Control[F].print(s"Loading ${statements.base}").liftA

      _ <- SQL[F].executeUpdate(env, Common.BeginTransaction)
      _ <- loadTransaction
      _ <- SQL[F].executeUpdate(env, Common.CommitTransaction)
      _ <- Control[F].print(s"Loading finished for ${statements.base}").liftA
      _ <- vacuum[F](env, statements)
      _ <- analyze[F](env, statements)
    } yield ()
  }

  /**
    * Get COPY action, either straight or transit (along with load manifest check)
    * @return
    */
  def getLoad[F[_]: Monad: Control: SQL](env: Environment[F], checkManifest: Boolean, dbSchema: String, copy: AtomicCopy, empty: Boolean): LoaderAction[F, Boolean] = {
    def check(eventsTable: EventsTable): LoaderAction[F, Boolean] =
      if (checkManifest) checkLoadManifest(env, dbSchema, eventsTable, empty) else LoaderAction.lift(false)

    copy match {
      case StraightCopy(copyStatement) => for {
        _ <- SQL[F].executeUpdate(env, copyStatement)
        emptyLoad <- check(AtomicEvents(dbSchema))
      } yield emptyLoad
      case TransitCopy(copyStatement) =>
        val create = RedshiftLoadStatements.createTransitTable(dbSchema)
        val destroy = RedshiftLoadStatements.destroyTransitTable(dbSchema)
        for {
          _ <- SQL[F].executeUpdate(env, create)
          // TODO: Transit copy provides more reliable empty-check
          emptyLoad <- check(TransitTable(dbSchema))
          _ <- SQL[F].executeUpdate(env, copyStatement)
          _ <- SQL[F].executeUpdate(env, destroy)
        } yield emptyLoad
    }
  }

  /**
   * Return action executing VACUUM statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def analyze[F[_]: Monad: Control: SQL](env: Environment[F], statements: RedshiftLoadStatements): LoaderAction[F, Unit] =
    statements.analyze match {
      case Some(analyze) =>
        for {
          _ <- SQL[F].executeTransaction(env, analyze)
          _ <- Control[F].print("ANALYZE transaction executed").liftA
        } yield ()
      case None => Control[F].print("ANALYZE transaction skipped").liftA
    }

  /**
   * Return action executing ANALYZE statements if there's any vacuum statements,
   * or noop if no vacuum statements were generated
   */
  def vacuum[F[_]: Monad: Control: SQL](env: Environment[F], statements: RedshiftLoadStatements): LoaderAction[F, Unit] = {
    statements.vacuum match {
      case Some(vacuum) =>
        val block = SqlString.unsafeCoerce("END") :: vacuum
        val actions = for {
          statement <- block
        } yield for {
          _ <- Control[F].print(statement).liftA
          _ <- SQL[F].executeUpdate(env, statement)
        } yield ()
        actions.sequence.void
      case None => Control[F].print("VACUUM queries skipped").liftA
    }
  }

  private val EmptyMessage = "Not adding record to load manifest as atomic data seems to be empty"
}
