package pl.epsilondeltalimit

case class FirstWriteWinsMap[K, V](underlying: Seq[(K, V)]) {
  def add(key: K, value: => V): FirstWriteWinsMap[K, V] =
    if (underlying.exists { case (k, _) => k == key }) this
    else FirstWriteWinsMap(underlying :+ (key, value))
}

object FirstWriteWinsMap {
  def empty[K, V]: FirstWriteWinsMap[K, V] = FirstWriteWinsMap(Seq.empty[(K, V)])
}
