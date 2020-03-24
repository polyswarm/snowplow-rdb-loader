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

import scala.util.control.NonFatal

import cats.{Id, Monad}
import cats.data.EitherT
import cats.implicits._

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest

import com.jcraft.jsch.{JSch, Session}

import com.snowplowanalytics.snowplow.rdbloader.{Environment, LoaderAction, LoaderError}
import com.snowplowanalytics.snowplow.rdbloader.common.StorageTarget.TunnelConfig

trait SSH[F[_]] {
  /** Create SSH tunnel to bastion host */
  def establishTunnel(tunnelConfig: TunnelConfig): LoaderAction[F, Unit]

  /** Close single available SSH tunnel */
  def closeTunnel(env: Environment[F]): LoaderAction[F, Unit]

  /** Retrieve decrypted property from EC2 Parameter Store */
  def getEc2Property(name: String): F[Either[LoaderError, String]]
}

object SSH {

  /** Actual SSH identity data. Both passphrase and key are optional */
  case class Identity(passphrase: Option[Array[Byte]], key: Option[Array[Byte]])

  /** Tunnel configuration with retrieved identity, ready to be used for establishing tunnel */
  case class Tunnel(config: TunnelConfig, identity: Identity)

  def apply[F[_]](implicit ev: SSH[F]): SSH[F] = ev

  /** Convert pure tunnel configuration to configuration with actual key and passphrase */
  def getIdentity[F[_]: Monad: SSH](tunnelConfig: TunnelConfig): LoaderAction[F, Identity] = {
    val key = tunnelConfig.bastion.key.map(_.ec2ParameterStore.parameterName).map(SSH[F].getEc2Property)
    // Invert Option, Either and Action
    val keyBytes = EitherT(key.sequence.map(_.sequence.map(_.map(_.getBytes))))
    keyBytes.map { key =>
      Identity(tunnelConfig.bastion.passphrase.map(_.getBytes()), key)
    }
  }

  /** Perform loading and make sure tunnel is closed */
  def bracket[F[_]: Monad: SSH, A](env: Environment[F], tunnelConfig: Option[TunnelConfig], action: LoaderAction[F, A]): LoaderAction[F, A] =
    tunnelConfig match {
      case Some(tunnel) => for {
        identity <- getIdentity[F](tunnel)
        _ <- SSH[F].establishTunnel(env, Tunnel(tunnel, identity))
        result <- action
        _ <- SSH[F].closeTunnel(env)
      } yield result
      case None => action
    }

  val sshInterpreter: SSH[Id] = new SSH[Id] {

    private var sshSession: Session = _

    /**
     * Create a SSH tunnel to bastion host and set port forwarding to target DB
     * @param tunnel SSH-tunnel configuration
     * @return either nothing on success and error message on failure
     */
    def establishTunnel(tunnelConfig: TunnelConfig): LoaderAction[Id, Option[Session]] = {
      
      env.ssh match {
        case None =>

          val result = Either.catchNonFatal {
            val jsch = new JSch()
            jsch.addIdentity("rdb-loader-tunnel-key", tunnel.identity.key.orNull, null, tunnel.identity.passphrase.orNull)
            sshSession = jsch.getSession(tunnel.config.bastion.user, tunnel.config.bastion.host, tunnel.config.bastion.port)
            sshSession.setConfig("StrictHostKeyChecking", "no")
            sshSession.connect()
            val _ = sshSession.setPortForwardingL(tunnel.config.localPort, tunnel.config.destination.host, tunnel.config.destination.port)
          }
          EitherT.fromEither[Id](result).leftMap(e => LoaderError.LoaderLocalError(s"Error during establishing SSH tunnel: ${e.getMessage}"))
        case Some(session) if session.isConnected =>
          EitherT.leftT(LoaderError.LoaderLocalError("Session for SSH tunnel already opened and connection is active"))
        case Some(_) =>
          EitherT.leftT(LoaderError.LoaderLocalError("Session for SSH tunnel already opened"))
      }
    }

    /** Try to close SSH tunnel, fail if it was not open */
    def closeTunnel(env: Environment[Id]): LoaderAction[Id, Unit] =
      env.ssh match {
        case None => EitherT.leftT(LoaderError.LoaderLocalError("Attempted to close nonexistent SSH session"))
        case Some(session) =>
          try {
            EitherT.rightT(session.disconnect())
          } catch {
            case NonFatal(e) => EitherT.leftT(LoaderError.LoaderLocalError(e.getMessage))
          }
      }

    /**
     * Get value from AWS EC2 Parameter Store
     * @param name systems manager parameter's name with SSH key
     * @return decrypted string with key
     */
    def getEc2Property(name: String): Id[Either[LoaderError, String]] =
      try {
        val client = AWSSimpleSystemsManagementClientBuilder.defaultClient()
        val req: GetParameterRequest = new GetParameterRequest().withName(name).withWithDecryption(true)
        val par = client.getParameter(req)
        Right(par.getParameter.getValue)
      } catch {
        case NonFatal(e) => Left(LoaderError.LoaderLocalError(e.getMessage))
      }
  }
}

