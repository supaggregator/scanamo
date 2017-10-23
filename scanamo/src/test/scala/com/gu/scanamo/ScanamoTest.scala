package com.gu.scanamo

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._

class ScanamoTest extends org.scalatest.FunSpec with org.scalatest.Matchers {
  it("should bring back all results for queries over large datasets") {
    val client = LocalDynamoDB.client()
    LocalDynamoDB.createTable(client)("large-query")('name -> S, 'number -> N)

    case class Large(name: String, number: Int, stuff: String)
    Scanamo.putAll(client)("large-query")(
      (for { i <- 0 until 100 } yield Large("Harry", i, util.Random.nextString(5000))).toSet
    )
    Scanamo.put(client)("large-query")(Large("George", 1, "x"))
    import syntax._
    Scanamo.query[Large](client)("large-query")('name -> "Harry").toList.size should be (100)

    client.deleteTable("large-query")
  }

  it("should get consistently") {
    val client = LocalDynamoDB.client()
    case class City(name: String, country: String)
    LocalDynamoDB.usingTable(client)("asyncCities")('name -> S) {

      Scanamo.put(client)("asyncCities")(City("Nashville", "US"))

      import com.gu.scanamo.syntax._
      Scanamo.getWithConsistency[City](client)("asyncCities")('name -> "Nashville") should equal(Some(Right(City("Nashville", "US"))))
    }
  }

  it("should get consistent") {
    case class City(name: String, country: String)

    val cityTable = Table[City]("asyncCities")

    import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._

    val client = LocalDynamoDB.client()
    LocalDynamoDB.usingTable(client)("asyncCities")('name -> S) {
      import com.gu.scanamo.syntax._
      val ops = for {
        _ <- cityTable.put(City("Nashville", "US"))
        res <- cityTable.consistently.get('name -> "Nashville")
      } yield res
      Scanamo.exec(client)(ops) should equal(Some(Right(City("Nashville", "US"))))
    }
  }

  it("should handle complex filters") {

    import com.gu.scanamo.syntax._
    import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._

    case class Game(
      userId: String,
      rangeKey: String,
      gameId: String,
      numTries: Option[Int],
      numCorrect: Option[Int],
      score: Option[Int],
      responseTime: Option[Long]
    )
    val table = Table[Game]("games")

    val client = LocalDynamoDB.client()
    LocalDynamoDB.usingTable(client)("games")('userId -> S, 'rangeKey -> S) {
      val games = Scanamo.exec(client)(for {
        _ <- table.putAll(Set(
          Game("user1", "1", "game1", Some(6), Some(1), Some(1), Some(1)),
          Game("user1", "2", "game2", Some(3), Some(1), Some(1), Some(1))
        ))
        results <- table
          .filter(
            attributeExists('numTries)
              and attributeExists('numCorrect)
              and attributeExists('score)
              and attributeExists('responseTime)
              and ('gameId -> "game1")
              and 'responseTime > 0
              and 'numTries > 5

          )
          .limit(10)
          .query('userId -> "user1")
      } yield results).flatMap(_.toOption)
      games shouldBe (List(Game("user1", "1", "game1", Some(6), Some(1), Some(1), Some(1))))
    }
  }
}
