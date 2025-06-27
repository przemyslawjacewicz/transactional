package pl.epsilondeltalimit

object ThrowWhenNot {
  def throwWhenNot[T](success: T)(o: => T): Unit = {
    val isSuccess = o

    if (isSuccess != success) {
      throw new RuntimeException("Operation finished with non-success value!")
    }
  }

  def throwWhenNotTrue(o: => Boolean): Unit =
    throwWhenNot(true)(o)
}
