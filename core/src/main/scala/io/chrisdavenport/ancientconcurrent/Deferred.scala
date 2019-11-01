/*
 * Copyright (c) 2017-2019 The Typelevel Cats-effect Project Developers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.chrisdavenport.ancientconcurrent

import cats._
import cats.effect._
import io.chrisdavenport.ancientconcurrent.internal.{LinkedMap, TrampolineEC}

import io.chrisdavenport.ancientconcurrent.Deferred.TransformedDeferred

import scala.concurrent.{ExecutionContext, Promise}
import scala.util.{Failure, Success}

/**
 * A purely functional synchronization primitive which represents a single value
 * which may not yet be available.
 *
 * When created, a `Deferred` is empty. It can then be completed exactly once,
 * and never be made empty again.
 *
 * `get` on an empty `Deferred` will block until the `Deferred` is completed.
 * `get` on a completed `Deferred` will always immediately return its content.
 *
 * `complete(a)` on an empty `Deferred` will set it to `a`, and notify any and
 * all readers currently blocked on a call to `get`.
 * `complete(a)` on a `Deferred` that has already been completed will not modify
 * its content, and result in a failed `F`.
 *
 * Albeit simple, `Deferred` can be used in conjunction with [[Ref]] to build
 * complex concurrent behaviour and data structures like queues and semaphores.
 *
 * Finally, the blocking mentioned above is semantic only, no actual threads are
 * blocked by the implementation.
 */
abstract class Deferred[F[_], A] {

  /**
   * Obtains the value of the `Deferred`, or waits until it has been completed.
   * The returned value may be canceled.
   */
  def get: F[A]

  /**
   * If this `Deferred` is empty, sets the current value to `a`, and notifies
   * any and all readers currently blocked on a `get`.
   *
   * Note that the returned action may complete after the reference
   * has been successfully set: use `F.start(r.complete)` if you want
   * asynchronous behaviour.
   *
   * If this `Deferred` has already been completed, the returned
   * action immediately fails with an `IllegalStateException`. In the
   * uncommon scenario where this behavior is problematic, you can
   * handle failure explicitly using `attempt` or any other
   * `ApplicativeError`/`MonadError` combinator on the returned
   * action.
   *
   * Satisfies:
   *   `Deferred[F, A].flatMap(r => r.complete(a) *> r.get) == a.pure[F]`
   */
  def complete(a: A): F[Unit]

  /**
   * Modify the context `F` using transformation `f`.
   */
  def mapK[G[_]](f: F ~> G): Deferred[G, A] =
    new TransformedDeferred(this, f)
}

abstract class TryableDeferred[F[_], A] extends Deferred[F, A] {

  /**
   * Obtains the current value of the `Deferred`, or None if it hasn't completed.
   */
  def tryGet: F[Option[A]]
}

object Deferred {
  /**
   * Creates an unset promise that only requires an [[Async]] and
   * does not support cancellation of `get`.
   *
   * WARN: some `Async` data types, like [[IO]], can be cancelable,
   * making `uncancelable` values unsafe. Such values are only useful
   * for optimization purposes, in cases where the use case does not
   * require cancellation or in cases in which an `F[_]` data type
   * that does not support cancellation is used.
   */
  def uncancelable[F[_], A](implicit F: Async[F]): F[Deferred[F, A]] =
    F.delay(unsafeUncancelable[F, A])

  /** Like [[uncancelable]] but initializes state using another effect constructor */
  def uncancelableIn[F[_], G[_], A](implicit F: Sync[F], G: Async[G]): F[Deferred[G, A]] =
    F.delay(unsafeUncancelable[G, A])

  /**
   * Creates an unset tryable promise that only requires an [[Async]] and
   * does not support cancellation of `get`.
   */
  def tryableUncancelable[F[_], A](implicit F: Async[F]): F[TryableDeferred[F, A]] =
    F.delay(unsafeTryableUncancelable[F, A])

  /**
   * Like [[uncancelable]] but returns the newly allocated promise directly
   * instead of wrapping it in `F.delay`. This method is considered
   * unsafe because it is not referentially transparent -- it
   * allocates mutable state.
   *
   * WARN: read the caveats of [[uncancelable]].
   */
  def unsafeUncancelable[F[_]: Async, A]: Deferred[F, A] = unsafeTryableUncancelable[F, A]

  private def unsafeTryableUncancelable[F[_]: Async, A]: TryableDeferred[F, A] =
    new UncancelableDeferred[F, A](Promise[A]())

  final private class Id

  sealed abstract private class State[A]
  private object State {
    final case class Set[A](a: A) extends State[A]
    final case class Unset[A](waiting: LinkedMap[Id, A => Unit]) extends State[A]
  }

  final private class UncancelableDeferred[F[_], A](p: Promise[A])(implicit F: Async[F]) extends TryableDeferred[F, A] {
    def get: F[A] =
      F.async { cb =>
        implicit val ec: ExecutionContext = TrampolineEC.immediate
        p.future.onComplete {
          case Success(a) => cb(Right(a))
          case Failure(t) => cb(Left(t))
        }
      }

    def tryGet: F[Option[A]] =
      F.delay(p.future.value.flatMap(_.toOption))

    def complete(a: A): F[Unit] =
      F.map(asyncBoundary) { _ =>
        p.success(a); ()
      }

    private[this] val asyncBoundary =
      F.async[Unit](cb => cb(Right(())))
  }

  final private class TransformedDeferred[F[_], G[_], A](underlying: Deferred[F, A], trans: F ~> G)
      extends Deferred[G, A] {
    override def get: G[A] = trans(underlying.get)
    override def complete(a: A): G[Unit] = trans(underlying.complete(a))
  }
}