package com.gu.scanamo

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.request.{ScanamoQueryOptions, ScanamoScanRequest}


class ResultsPage[T: DynamoFormat](
  val results: List[Either[DynamoReadError, T]],
  nextKey: Option[java.util.Map[String, AttributeValue]],
  tableName: String
) {
  import collection.JavaConverters._

  def nextPage(): Option[ScanamoOps[ResultsPage[T]]] = nextKey.map { key =>
    val format = implicitly[DynamoFormat[T]]
    for {
      results <- ScanamoOps.scan(ScanamoScanRequest(tableName, None,
        ScanamoQueryOptions.default.copy(exclusiveStartKey = Some(key.asScala.toMap))))
    } yield {
      new ResultsPage[T](results = results.getItems.asScala.toList.map(av =>
        format.read(new AttributeValue().withM(av))), Option(results.getLastEvaluatedKey),
        tableName
      )
    }
  }
}
