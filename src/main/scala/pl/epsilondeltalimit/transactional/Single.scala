package pl.epsilondeltalimit.transactional

trait Single[S, O, B] {
  val source: S
  val output: S => O
  val backup: S => B
  val restore: B => S => Unit
  val cleanup: B => Unit //todo: check if we need this
}
