package com.awssdk2.stream.sample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.Select;
import software.amazon.awssdk.services.dynamodb.paginators.QueryPublisher;

@RestController
public class DemoController {

  @GetMapping(path = "/address/tokyo", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
  public List<Address> getAddress() {

    AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard().build();
    Map<String, AttributeValue> lastEvaluatedKey = null;
    List<Address> responseList = new ArrayList<Address>();

    // loop to get all pages 
    do {

      // create QueryResult
      Map<String, Condition> expressionAttributeValues = new HashMap<String, Condition>();
      Condition condition = new Condition();
      condition.withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(new AttributeValue().withN("13"));
      expressionAttributeValues.put("prefectureCode", condition);
      QueryRequest queryRequest = new QueryRequest().withTableName("address-list")
          .withExclusiveStartKey(lastEvaluatedKey).withIndexName("Index-prefectureCode")
          .withKeyConditions(expressionAttributeValues);
      QueryResult queryResult = dbClient.query(queryRequest);

      queryResult.getItems().stream().forEach(result -> {
        Address i = new Address();
        i.setAddressCode(result.get("addressCode") != null ? result.get("addressCode").getN() : "");
        i.setZipCode(result.get("zipCode") != null ? result.get("zipCode").getS() : "");
        i.setPrefectureCode(result.get("prefectureCode") != null ? result.get("prefectureCode").getN() : "");
        i.setPrefectureNama(result.get("prefectureNama") != null ? result.get("prefectureNama").getS() : "");
        i.setCityName(result.get("cityName") != null ? result.get("cityName").getS() : "");
        i.setDistrictName(result.get("districtName") != null ? result.get("districtName").getS() : "");
        i.setBlockName(result.get("blockName") != null ? result.get("blockName").getS() : "");
        responseList.add(i);
      });

      // loop until lastEvaluatedKey is empty
      lastEvaluatedKey = queryResult.getLastEvaluatedKey();
    } while (lastEvaluatedKey != null);

    return responseList;
  }

  @GetMapping(path = "/address/tokyo", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
  public Flux<Address> getAddressWithStream() {

    // not need to set exclusiveStartKey because of getting the next page of results automatically
    QueryPublisher queryPublisher = DynamoDbAsyncClient
                                      .create()
                                      .queryPaginator(request -> {
                                        request.tableName("address-list")
                                        .select(Select.ALL_ATTRIBUTES)
                                        .indexName("Index-prefectureCode")
                                        .keyConditionExpression("prefectureCode = :tokyo")
                                        .expressionAttributeValues(Map.of(":tokyo",
                                          software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n("13").build()));
                                      });

    return Flux.from(
      queryPublisher.items().map(map -> {
        Address i = new Address();
        i.setAddressCode(map.get("addressCode") != null ? map.get("addressCode").n() : "");
        i.setZipCode(map.get("zipCode") != null ? map.get("zipCode").s() : "");
        i.setPrefectureCode(map.get("prefectureCode") != null ? map.get("prefectureCode").n() : "");
        i.setPrefectureNama(map.get("prefectureNama") != null ? map.get("prefectureNama").s() : "");
        i.setCityName(map.get("cityName") != null ? map.get("cityName").s() : "");
        i.setDistrictName(map.get("districtName") != null ? map.get("districtName").s() : "");
        i.setBlockName(map.get("blockName") != null ? map.get("blockName").s() : "");
        return i;
      })
    );
  }
}