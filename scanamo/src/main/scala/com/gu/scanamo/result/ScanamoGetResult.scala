package org.scanamo.result

sealed trait ScanamoGetResult[+A] {
  def toOption: Option[A]
}

object ScanamoGetResult {

  case class ScanamoSingleGetResult[A] private (a: A) extends ScanamoGetResult[A] {
    def toOption = Option(a)
  }
  case object Empty extends ScanamoGetResult[Nothing] {
    def toOption = None
  }

  def apply[A](a: A): ScanamoGetResult[A] = ScanamoSingleGetResult(a)
}

