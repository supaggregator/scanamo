package com.gu.scanamo

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.scalatest.{FunSuite, Matchers}

class SomeDynamoFormatTest extends FunSuite with Matchers {
  import com.gu.scanamo.generic.auto._

  test("automatic derivation for case object should only work if treating it as an enum") {
    write[Option[String]](Some("foo")) shouldBe (write(Some("foo")))
  }

  def write[T](t: T)(implicit f: DynamoFormat[T]) = f.write(t)
}

