package pl.epsilondeltalimit.transactional

import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

object Transactional extends LazyLogging {

  def transactional[S, O, B](s: Single[S, O, B], ss: Single[S, O, B]*): Try[Seq[O]] = {
    val sss = s +: ss
    logger.info(s"[TX:START] Starting operations on sources: ${sss.map(_.source).mkString(",")}.")

    val (updated, result) =
      sss.foldLeft((FirstWriteWinsMap[S, (B, B => Unit)](Seq.empty), Success(Seq.empty[O]): Try[Seq[O]])) {
        case (acc, single) =>
          acc match {
            case (_, _: Failure[_]) =>
              acc
            case (updated, Success(os)) =>
              val newUpdated = updated.add(single.source, (single.backup(single.source), single.cleanup))
              Try(single.output(single.source)) match {
                case Failure(ex) =>
                  logger.error(s"[TX] Operation failed for source: ${single.source}. Restoring sources.", ex)
                  updated.underlying match {
                    case underlying if underlying.nonEmpty =>
                      logger.warn(s"[TX] Restoring sources: ${updated.underlying.map(_._1).mkString(",")}.")
                      updated.underlying.foreach { case (s, (b, _)) =>
                        Try(single.restore(b)(s)) match {
                          case Failure(ex) =>
                            logger.error(s"[TX] Failed to restore source: source=$s, backup=$b.", ex)
                          case Success(_) =>
                            logger.info(s"[TX] Successfully restored source: source=$s, backup=$b.")
                        }
                      }
                    case _ =>
                      logger.warn(s"[TX] No sources updated, nothing to restore.")
                  }
                  (newUpdated, Failure(ex))
                case Success(o) =>
                  logger.info(s"[TX] Operation succeeded for source: ${single.source}.")
                  (newUpdated, Success(o +: os))
              }
          }
      }

    updated.underlying.foreach { case (source, (backup, getCleanup)) =>
      logger.info(s"[TX] Cleaning up backup for source: backup=$backup, source=$source.")
      Try(getCleanup(backup)) match {
        case Failure(ex) =>
          logger.error(s"[TX] Failed to cleanup backup for source: backup=$backup, source=$source.", ex)
        case Success(_) =>
          logger.info(s"[TX] Successfully cleaned up backup for source: backup=$backup, source=$source.")
      }
    }

    result match {
      case _: Failure[_] =>
        logger.error(s"[TX:END_FAILURE] Finished operations on sources: ${sss.map(_.source).mkString(",")}.")
      case _: Success[_] =>
        logger.info(s"[TX:END_SUCCESS] Finished operations on sources: ${sss.map(_.source).mkString(",")}.")
    }
    result
  }

  private case class FirstWriteWinsMap[K, V](underlying: Seq[(K, V)]) {
    def add(key: K, value: => V): FirstWriteWinsMap[K, V] =
      if (underlying.exists { case (k, _) => k == key }) this
      else FirstWriteWinsMap(underlying :+ (key, value))
  }

}
