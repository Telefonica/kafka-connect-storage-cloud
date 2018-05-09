package com.telefonica.kafka.connect.s3.storage.partitioner;

import io.confluent.connect.storage.partitioner.TimestampExtractor;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
                if (fieldValue == null) {
                    fieldValue = "null";
                }
                partition.append(fieldName + "=" + fieldValue.toString() + delim);
            }
        } else {
            log.error("Value is not of Struct or Map type.");
            throw new PartitionException("Error encoding partition.");
        }

        partition.append(super.encodePartition(sinkRecord));
        return partition.toString();
    }

    public TimestampExtractor newTimestampExtractor(String extractorClassName) {
        try {
            byte var5 = -1;
            switch (extractorClassName.hashCode()) {
                case -1851041679:
                    if (extractorClassName.equals("Record")) {
                        var5 = 1;
                    }
                    break;
                case -1333070071:
                    if (extractorClassName.equals("RecordField")) {
                        var5 = 2;
                    }
                    break;
                case -827983772:
                    if (extractorClassName.equals("Wallclock")) {
                        var5 = 0;
                    }
            }

            switch (var5) {
                case 0:
                case 1:
                case 2:
                    extractorClassName = "com.telefonica.kafka.connect.s3.storage.partitioner.MultiFieldDailyPartitioner$" + extractorClassName + "TimestampExtractor";
                default:
                    Class<?> klass = Class.forName(extractorClassName);
                    if (!TimestampExtractor.class.isAssignableFrom(klass)) {
                        throw new ConnectException("Class " + extractorClassName + " does not implement TimestampExtractor");
                    } else {
                        return (TimestampExtractor) klass.newInstance();
                    }
            }
        } catch (ClassCastException | IllegalAccessException | InstantiationException | ClassNotFoundException var4) {
            ConfigException ce = new ConfigException("Invalid timestamp extractor: " + extractorClassName);
            ce.initCause(var4);
            throw ce;
        }
    }

    public static class RecordFieldTimestampExtractor implements TimestampExtractor {
        private String fieldName;
        private DateTimeFormatter dateTime;

        public RecordFieldTimestampExtractor() {
        }

        public void configure(Map<String, Object> config) {
            this.fieldName = (String) config.get("timestamp.field");
            this.dateTime = ISODateTimeFormat.dateTimeParser();
        }

        public Long extract(ConnectRecord<?> record) {
            Object value = record.value();
            Object timestampValue;
            if (value instanceof Struct) {
                Struct struct = (Struct) value;
                timestampValue = struct.get(this.fieldName);
                Schema valueSchema = record.valueSchema();
                Schema fieldSchema = valueSchema.field(this.fieldName).schema();
                if ("org.apache.kafka.connect.data.Timestamp".equals(fieldSchema.name())) {
                    return ((Date) timestampValue).getTime();
                } else {
                    switch (fieldSchema.type()) {
                        case INT32:
                        case INT64:
                            return ((Number) timestampValue).longValue();
                        case STRING:
                            return this.dateTime.parseMillis((String) timestampValue);
                        default:
                            MultiFieldDailyPartitioner.log.error("Unsupported type '{}' for user-defined timestamp field.", fieldSchema.type().getName());
                            throw new PartitionException("Error extracting timestamp from record field: " + this.fieldName);
                    }
                }
            } else if (value instanceof Map) {
                Map<?, ?> map = (Map) value;
                timestampValue = map.get(this.fieldName);
                if (timestampValue instanceof Number) {
                    return ((Number) timestampValue).longValue();
                } else if (timestampValue instanceof String) {
                    return this.dateTime.parseMillis((String) timestampValue);
                } else if (timestampValue instanceof Date) {
                    return ((Date) timestampValue).getTime();
                } else {
                    MultiFieldDailyPartitioner.log.error("Unsupported type '{}' for user-defined timestamp field.", timestampValue.getClass());
                    throw new PartitionException("Error extracting timestamp from record field: " + this.fieldName);
                }
            } else {
                MultiFieldDailyPartitioner.log.error("Value is not of Struct or Map type.");
                throw new PartitionException("Error encoding partition.");
            }
        }
    }

    public static class RecordTimestampExtractor implements TimestampExtractor {
        public RecordTimestampExtractor() {
        }

        public void configure(Map<String, Object> config) {
        }

        public Long extract(ConnectRecord<?> record) {
            return record.timestamp();
        }
    }

    public static class WallclockTimestampExtractor implements TimestampExtractor {
        public WallclockTimestampExtractor() {
        }

        public void configure(Map<String, Object> config) {
        }

        public Long extract(ConnectRecord<?> record) {
            return Time.SYSTEM.milliseconds();
        }
    }
}
