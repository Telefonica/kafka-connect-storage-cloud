package com.telefonica.kafka.connect.azblob.storage.partitioner;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

public class MultiFieldDailyPartitioner<T> extends TimeBasedPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(MultiFieldDailyPartitioner.class);
  private List<String> fieldNames;

  @Override
  public void configure(Map<String, Object> config) {
    String delim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
    String pathFormat = "'year'=YYYY" + delim + "'month'=MM" + delim + "'day'=dd";

    String localeString = (String) config.get(PartitionerConfig.LOCALE_CONFIG);
    if (localeString.equals("")) {
      throw new ConfigException(
              PartitionerConfig.LOCALE_CONFIG,
              localeString,
              "Locale cannot be empty."
      );
    }

    String timeZoneString = (String) config.get(PartitionerConfig.TIMEZONE_CONFIG);
    if (timeZoneString.equals("")) {
      throw new ConfigException(
              PartitionerConfig.TIMEZONE_CONFIG,
              timeZoneString,
              "Timezone cannot be empty."
      );
    }

    Locale locale = new Locale(localeString);
    DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);

    init(TimeUnit.DAYS.toMillis(1), pathFormat, locale, timeZone, config);

    String fieldNamesString = (String) config.get(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG);
    fieldNames = Arrays.asList(fieldNamesString.split(","));

    StringBuilder format = new StringBuilder();
    format.append(pathFormat + delim);
    for (String field : fieldNames) {
      format.append(field + delim);
    }

    if (format.length() > 0) {
      format.delete(format.length() - delim.length(), format.length());
    }

    partitionFields = newSchemaGenerator(config).newPartitionFields(format.toString());
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    Object value = sinkRecord.value();
    Schema valueSchema = sinkRecord.valueSchema();
    StringBuilder partition = new StringBuilder();
    partition.append(super.encodePartition(sinkRecord) + delim);

    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      for (String fieldName : fieldNames) {
        Object partitionKey = struct.get(fieldName);
        Type type = valueSchema.field(fieldName).schema().type();
        switch (type) {
          case INT8:
          case INT16:
          case INT32:
          case INT64:
            Number record = (Number) partitionKey;
            partition.append(fieldName + "=" + record.toString() + delim);
            break;
          case STRING:
            partition.append(fieldName + "=" + (String) partitionKey + delim);
            break;
          case BOOLEAN:
            boolean booleanRecord = (boolean) partitionKey;
            partition.append(fieldName + "=" + Boolean.toString(booleanRecord) + delim);
            break;
          default:
            log.error("Type {} is not supported as a partition key.", type.getName());
            throw new PartitionException("Error encoding partition.");
        }
      }
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      for (String fieldName : fieldNames) {
        Object fieldValue = map.get(fieldName);
        partition.append(fieldName + "=" + fieldValue.toString() + delim);
      }
    } else {
      log.error("Value is not of Struct or Map type.");
      throw new PartitionException("Error encoding partition.");
    }

    if (partition.length() > 0) {
      partition.delete(partition.length() - delim.length(), partition.length());
    }
    return partition.toString();
  }
}
