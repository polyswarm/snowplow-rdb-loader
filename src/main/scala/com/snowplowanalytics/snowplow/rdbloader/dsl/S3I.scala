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

import java.nio.file.{Files, Path, Paths}

import scala.collection.convert.wrapAsScala._
import scala.util.control.NonFatal

import cats.{Functor, Id}
import cats.implicits._

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

// This project
import com.snowplowanalytics.snowplow.rdbloader.{LoaderError, S3, LoaderAction, Environment}
import com.snowplowanalytics.snowplow.rdbloader.S3.{splitS3Key, splitS3Path}
import com.snowplowanalytics.snowplow.rdbloader.config.SnowplowConfig.SnowplowAws
import com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure.{S3Failure, DownloadFailure}


trait S3I[F[_]] {

  def listS3(env: Environment[F], bucket: S3.Folder): F[Either[LoaderError, List[S3.BlobObject]]]

  /** Check if S3 key exist */
  def keyExists(env: Environment[F], key: S3.Key): F[Boolean]

  /** Download S3 key into local path */
  def downloadData(env: Environment[F], source: S3.Folder, dest: Path): LoaderAction[F, List[Path]]
}

object S3I {
  def apply[F[_]](implicit ev: S3I[F]): S3I[F] = ev


  /**
   * Create S3 client, backed by AWS Java SDK
   *
   * @param awsConfig Snowplow AWS Configuration
   * @return Snowplow-specific S3 client
   */
  def getClient(awsConfig: SnowplowAws): AmazonS3 =
    AmazonS3ClientBuilder.standard().withRegion(awsConfig.s3.region).build()

  val s3Interpreter = new S3I[Id] {

    private val F = Functor[Either[LoaderError, ?]].compose[List]

    def list(client: AmazonS3, str: S3.Folder) = {
      val (bucket, prefix) = splitS3Path(str)

      val req = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withPrefix(prefix)

      def keyUnfold(result: ListObjectsV2Result): Stream[S3ObjectSummary] = {
        if (result.isTruncated) {
          val loaded = result.getObjectSummaries()
          req.setContinuationToken(result.getNextContinuationToken)
          loaded.toStream #::: keyUnfold(client.listObjectsV2(req))
        } else {
          result.getObjectSummaries().toStream
        }
      }

      try {
        Right(keyUnfold(client.listObjectsV2(req)).filterNot(_.getSize == 0).toList)
      } catch {
        case NonFatal(e) => Left(LoaderError.DiscoveryError(List(S3Failure(e.toString))))
      }

    }

    def listS3(env: Environment[Id], bucket: S3.Folder): Id[Either[LoaderError, List[S3.BlobObject]]] =
      list(env.s3, bucket).map(summaries => summaries.map(S3.getKey))

    /**
     * Check if some `file` exists in S3 `path`
     *
     * @param key valid S3 key (without trailing slash)
     * @return true if file exists, false if file doesn't exist or not available
     */
    def keyExists(env: Environment[Id], key: S3.Key): Id[Boolean] = {
      val (bucket, s3Key) = splitS3Key(key)
      val request = new GetObjectMetadataRequest(bucket, s3Key)
      try {
        env.s3.getObjectMetadata(request)
        true
      } catch {
        case _: AmazonServiceException => false
      }
    }

    /**
     * Download contents of S3 folder into `destination`
     *
     * @param source AWS S3 folder
     * @param dest optional local path, tmp dir will be used if not specified
     * @return list of downloaded filenames
     */
    def downloadData(env: Environment[Id], source: S3.Folder, dest: Path): LoaderAction[Id, List[Path]] = {
      val client: AmazonS3 = env.s3
      val files = F.map(list(client, source)) { summary =>
        val bucket = summary.getBucketName
        val key = summary.getKey
        try {
          val s3Object = client.getObject(new GetObjectRequest(bucket, key))
          val destinationFile = Paths.get(dest.toString, key)

          if (!Files.exists(destinationFile)) {
            Files.createDirectories(destinationFile.getParent)
            Files.copy(s3Object.getObjectContent, destinationFile)
            Right(destinationFile)
          } else {
            Left(DownloadFailure(S3.Key.coerce(s"s3://$bucket/$key"), "File already exist"))
          }
        } catch {
          case NonFatal(e) =>
            Left(DownloadFailure(S3.Key.coerce(s"s3://$bucket/$key"), e.toString))
        }
      }

      val result = files.map(stream => stream.sequence match {
        case Left(failure) => Left(LoaderError.DiscoveryError(List(failure)))
        case Right(success) => Right(success)
      }).flatten

      LoaderAction.liftE(result)
    }
  }
}

