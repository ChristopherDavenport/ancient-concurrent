package io.chrisdavenport.ancientconcurrent.keysemaphore

import cats._
import cats.data._
import cats.effect._
import cats.implicits._
import org.scalatest.{Assertion, EitherValues}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import io.chrisdavenport.ancientconcurrent._

class KeySemaphoreTests extends AsyncFunSuite with Matchers with EitherValues {

  implicit override def executionContext: ExecutionContext = ExecutionContext.Implicits.global

  def tests(label: String, sc: Long => IO[Semaphore[Kleisli[IO, Unit, ?]]]): Unit = {
    test(s"$label - acquire n synchronously") {
      val n = 20
      sc(20)
        .flatMap { s =>
          (0 until n).toList.traverse(_ => s.acquire.run(())).void *> s.available.run(())
        }
        .unsafeToFuture
        .map(_ shouldBe 0)
    }

    test(s"$label - tryAcquire with available permits") {
      val n = 20
      sc(30)
        .flatMap { s =>
          for {
            _ <- (0 until n).toList.traverse(_ => s.acquire.run(())).void
            t <- s.tryAcquire.run(())
          } yield t
        }
        .unsafeToFuture
        .map(_ shouldBe true)
    }

    test(s"$label - tryAcquire with no available permits") {
      val n = 20
      sc(20)
        .flatMap { s =>
          for {
            _ <- (0 until n).toList.traverse(_ => s.acquire.run(())).void
            t <- s.tryAcquire.run(())
          } yield t
        }
        .unsafeToFuture
        .map(_ shouldBe false)
    }

    // test(s"$label - offsetting acquires/releases - acquires parallel with releases") {
    //   testOffsettingReleasesAcquires((s, permits) => permits.traverse(n => s.acquireN(n).run(())).void,
    //                                  (s, permits) => permits.reverse.traverse(n => s.releaseN(n).run(())).void)
    // }

    // test(s"$label - offsetting acquires/releases - individual acquires/increment in parallel") {
    //   testOffsettingReleasesAcquires((s, permits) => permits.parTraverse(n => s.acquireN(n).run(())).void,
    //                                  (s, permits) => permits.reverse.parTraverse(n => s.releaseN(n).run(())).void)
    // }

    test(s"$label - available with available permits") {
      sc(20)
        .flatMap { s =>
          for {
            _ <- s.acquireN(19).run(())
            t <- s.available.run(())
          } yield t
        }
        .unsafeToFuture()
        .map(_ shouldBe 1)
    }

    test(s"$label - available with no available permits") {
      sc(20)
        .flatMap { s =>
          for {
            _ <- s.acquireN(20).run(()).void
            t <- IO.shift *> s.available.run(())
          } yield t

        }
        .unsafeToFuture()
        .map(_ shouldBe 0)
    }

    test(s"$label - tryAcquireN with no available permits") {
      sc(20)
        .flatMap { s =>
          for {
            _ <- s.acquireN(20).run(()).void
            _ <- s.acquire.run(()).start
            x <- (IO.shift *> s.tryAcquireN(1).run(())).start
            t <- x.join
          } yield t
        }
        .unsafeToFuture()
        .map(_ shouldBe false)
    }

    test(s"$label - count with available permits") {
      val n = 18
      sc(20)
        .flatMap { s =>
          for {
            _ <- (0 until n).toList.traverse(_ => s.acquire.run(())).void
            a <- s.available.run(())
            t <- s.count.run(())
          } yield (a, t)
        }
        .unsafeToFuture()
        .map { case (available, count) => available shouldBe count }
    }

    test(s"$label - count with no available permits") {
      sc(20)
        .flatMap { s =>
          for {
            _ <- s.acquireN(20).run(()).void
            x <- (IO.shift *> s.count.run(())).start
            t <- x.join
          } yield t
        }
        .unsafeToFuture()
        .map(count => count shouldBe 0)
    }

  //   def testOffsettingReleasesAcquires(acquires: (Semaphore[Kleisli[IO, Unit, *]], Vector[Long]) => IO[Unit],
  //                                      releases: (Semaphore[Kleisli[IO, Unit, *]], Vector[Long]) => IO[Unit]): Future[Assertion] = {
  //     val permits: Vector[Long] = Vector(1, 0, 20, 4, 0, 5, 2, 1, 1, 3)
  //     sc(0)
  //       .flatMap { s =>
  //         (acquires(s, permits) *> IO.shift, releases(s, permits) *> IO.shift).parTupled *> s.count.run(())
  //       }
  //       .unsafeToFuture
  //       .map(_ shouldBe 0L)
  //   }
  }

  // tests("concurrent", n => Semaphore[IO](n))

  // test("concurrent - acquire does not leak permits upon cancelation") {
  //   Semaphore[IO](1L)
  //     .flatMap { s =>
  //       // acquireN(2) will get 1 permit and then timeout waiting for another,
  //       // which should restore the semaphore count to 1. We then release a permit
  //       // bringing the count to 2. Since the old acquireN(2) is canceled, the final
  //       // count stays at 2.
  //       s.acquireN(2L).timeout(1.milli).attempt *> s.release *> IO.sleep(10.millis) *> s.count
  //     }
  //     .unsafeToFuture
  //     .map(_ shouldBe 2L)
  // }

  // test("concurrent - withPermit does not leak fibers or permits upon cancelation") {
  //   Semaphore[IO](0L)
  //     .flatMap { s =>
  //       // The inner s.release should never be run b/c the timeout will be reached before a permit
  //       // is available. After the timeout and hence cancelation of s.withPermit(...), we release
  //       // a permit and then sleep a bit, then check the permit count. If withPermit doesn't properly
  //       // cancel, the permit count will be 2, otherwise 1
  //       s.withPermit(s.release).timeout(1.milli).attempt *> s.release *> IO.sleep(10.millis) *> s.count
  //     }
  //     .unsafeToFuture
  //     .map(_ shouldBe 1L)
  // }

  tests("async", n => KeySemaphore.uncancelable[IO, Unit](_ => n))

  test("only take the maximum values per key"){
    val test = for {
      sem <- KeySemaphore.uncancelable[IO, Unit]{_: Unit => 1L}
      first <- sem.tryAcquire.run(())
      second <- sem.tryAcquire.run(())
    } yield (first, second)
    
    test
      .unsafeToFuture()
      .map(_ shouldBe ((true, false)))
  }

  test("not be affected by other keys"){
    val test = for {
      sem <- KeySemaphore.uncancelable[IO, Int]{_: Int => 1L}
      first <- sem.tryAcquire.run(1)
      second <- sem.tryAcquire.run(2)
      third <- sem.tryAcquire.run(1)
    } yield (first, second, third)
    test.unsafeToFuture()
    .map(_ shouldBe ((true, true, false)))
  }

  test("restore on finished") {
    val test = for {
      sem <- KeySemaphore.uncancelable[IO, Int]{_: Int => 1L}
      first <- sem.tryAcquire.run(1)
      second <- sem.tryAcquire.run(1)
      _ <- sem.release.run(1)
      third <- sem.tryAcquire.run(1)
    } yield (first, second, third)
    test.unsafeToFuture()
    .map(_ shouldBe ((true, false, true)))
  }

  test("not allow more than the key") {
    val test = for {
      sem <- KeySemaphore.uncancelable[IO, Int]{_: Int => 1L}
      first <- sem.tryAcquire.run(1)
      _ <- sem.releaseN(10).run(1)
      second <- sem.tryAcquire.run(1)
      third <- sem.tryAcquire.run(1)
    } yield (first, second, third)
    test.unsafeToFuture()
    .map(_ shouldBe ((true, true, false)))
  }


}