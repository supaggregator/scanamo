package com.gu.scanamo

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.format.DefaultDynamoFormats
import simulacrum.typeclass

/**
  * Type class for defining serialisation to and from
  * DynamoDB's `AttributeValue`
  *
  * {{{
  * >>> val listOptionFormat = DynamoFormat[List[Option[Int]]]
  * >>> listOptionFormat.read(listOptionFormat.write(List(Some(1), None, Some(3))))
  * Right(List(Some(1), None, Some(3)))
  * }}}
  *
  * Also supports automatic derivation for case classes
  *
  * {{{
  * >>> case class Farm(animals: List[String])
  * >>> case class Farmer(name: String, age: Long, farm: Farm)
  * >>> val farmerF = DynamoFormat[Farmer]
  * >>> farmerF.read(farmerF.write(Farmer("McDonald", 156L, Farm(List("sheep", "cow")))))
  * Right(Farmer(McDonald,156,Farm(List(sheep, cow))))
  * }}}
  *
  * and for sealed trait + case object hierarchies
  *
  * {{{
  * >>> sealed trait Animal
  * >>> case object Aardvark extends Animal
  * >>> case object Zebra extends Animal
  * >>> case class Pet(name: String, animal: Animal)
  * >>> val petF = DynamoFormat[Pet]
  * >>> petF.read(petF.write(Pet("Amy", Aardvark)))
  * Right(Pet(Amy,Aardvark))
  *
  * >>> petF.read(petF.write(Pet("Zebediah", Zebra)))
  * Right(Pet(Zebediah,Zebra))
  * }}}
  *
  * as well as more complex sealed trait + case class hierarchies
  *
  * {{{
  * >>> sealed trait Monster
  * >>> case object Godzilla extends Monster
  * >>> case class GiantSquid(tentacles: Int) extends Monster
  * >>> val monsterF = DynamoFormat[Monster]
  * >>> monsterF.read(monsterF.write(Godzilla))
  * Right(Godzilla)
  *
  * >>> monsterF.read(monsterF.write(GiantSquid(12)))
  * Right(GiantSquid(12))
  * }}}
  *
  * Problems reading a value are detailed
  * {{{
  * >>> import cats.syntax.either._
  *
  * >>> case class Developer(name: String, age: String, problems: Int)
  * >>> val invalid = DynamoFormat[Farmer].read(DynamoFormat[Developer].write(Developer("Alice", "none of your business", 99)))
  * >>> invalid
  * Left(InvalidPropertiesError(NonEmptyList(PropertyReadError(age,NoPropertyOfType(N,{S: none of your business,})), PropertyReadError(farm,MissingProperty))))
  *
  * >>> invalid.leftMap(cats.Show[error.DynamoReadError].show)
  * Left('age': not of type: 'N' was '{S: none of your business,}', 'farm': missing)
  * }}}
  *
  * Optional properties are defaulted to None
  * {{{
  * >>> case class LargelyOptional(a: Option[String], b: Option[String])
  * >>> DynamoFormat[LargelyOptional].read(DynamoFormat[Map[String, String]].write(Map("b" -> "X")))
  * Right(LargelyOptional(None,Some(X)))
  * }}}
  *
  * Custom formats can often be most easily defined using [[DynamoFormat.coercedXmap]], [[DynamoFormat.xmap]] or [[DynamoFormat.iso]]
  */
@typeclass trait DynamoFormat[T] {
  def read(av: AttributeValue): Either[DynamoReadError, T]
  def write(t: T): AttributeValue
  def default: Option[T] = None
}

object DynamoFormat extends DefaultDynamoFormats