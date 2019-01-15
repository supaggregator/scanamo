package org.scanamo.result

sealed trait ScanamoPutResult[+A] {
  def toOption: Option[A]
}

object ScanamoPutResult {

  case class ScanamoSinglePutResult[A] private (a: A) extends ScanamoPutResult[A] {
    def toOption = Option(a)
  }
  case object Empty extends ScanamoPutResult[Nothing] {
    def toOption = None
  }

  def apply[A](a: A): ScanamoPutResult[A] = ScanamoSinglePutResult(a)
}



