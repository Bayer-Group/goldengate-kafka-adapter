package com.monsanto.data.goldengate.encoder.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsToken;
import com.goldengate.atg.datasource.adapt.Col;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.google.common.base.Optional;
import com.monsanto.data.goldengate.GoldenGateConstants;
import com.monsanto.data.goldengate.config.Conf;
import com.monsanto.data.goldengate.config.TableConfiguration;
import com.monsanto.data.goldengate.encoder.AbstractMessageEncoder;
import com.monsanto.data.goldengate.encoder.exception.UnableToEncodeMessageException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

public class JsonEncoder extends AbstractMessageEncoder {
    private static final Logger log = LoggerFactory.getLogger(JsonEncoder.class);
    private static final String ORACLE_NULL = "NULL";

    private JsonFactory factory = new JsonFactory();
    private String ENV_TIMEZONE;

    public JsonEncoder(Conf configuration) {
        super(configuration);
        TimeZone timeZone = Calendar.getInstance().getTimeZone();
        ENV_TIMEZONE = timeZone.getDisplayName(false, TimeZone.SHORT);

        factory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    }

    @Override
    public byte[] encode(Tx tx, Op op) {
        try {
            ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
            JsonGenerator jsonGenerator = factory.createGenerator(outBytes);

            jsonGenerator.writeStartObject();

            writeOperationMetaData(op, jsonGenerator);
            writeColumnArray(op, jsonGenerator, op.getOpType());
            writeTokenArray(op, jsonGenerator);

            jsonGenerator.writeEndObject();

            jsonGenerator.flush();

            return outBytes.toByteArray();
        } catch (IOException e) {
            log.error("Error serializing operation to JSON. message: " + op.toString(), e);
            throw new UnableToEncodeMessageException("Error encoding message as JSON.", e);
        }
    }

    private void writeOperationMetaData(Op op, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeStringField(JsonFields.JSON_SCHEMA, op.getTableName().getSchemaName());
        jsonGenerator.writeStringField(JsonFields.JSON_TABLE, op.getTableName().getShortName());
        jsonGenerator.writeStringField(JsonFields.JSON_MODTYPE, String.valueOf(op.getOperationType().getCharID()));

        jsonGenerator.writeStringField(JsonFields.JSON_TIMESTAMP, op.getTimestamp());
        jsonGenerator.writeStringField(JsonFields.JSON_TIMEZONE, ENV_TIMEZONE);

        writeTransactionMetadata(op, jsonGenerator);
    }

    private void writeTransactionMetadata(Op op, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeNumberField(JsonFields.JSON_FILESEQNO, op.getSeqno());
        jsonGenerator.writeNumberField(JsonFields.JSON_FILERBA, op.getRba());
        jsonGenerator.writeNumberField(JsonFields.JSON_TRANSIND, op.getTxState().getState());

        writeNullableField(JsonFields.JSON_DBUSER, op.getEnv(GoldenGateConstants.ENV_TRANSACTION, GoldenGateConstants.ENV_USERNAME), jsonGenerator);
        writeNullableField(JsonFields.JSON_GGHOST, op.getEnv(GoldenGateConstants.ENV_GGENVIRONMENT, GoldenGateConstants.ENV_HOSTNAME), jsonGenerator);
        writeNullableField(JsonFields.JSON_TRANSID, op.getEnv(GoldenGateConstants.ENV_TRANSACTION, GoldenGateConstants.ENV_TRANSACTIONID), jsonGenerator);
    }

    private void writeTokenArray(Op op, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeArrayFieldStart(JsonFields.JSON_TOKENS);
        Optional<TableConfiguration> configurationForTable = getConfigurationForTable(op.getTableName());

        if (configurationForTable.isPresent()) {
            Map<String, String> tokenMap = new HashMap<>();
            for (Map.Entry<String,DsToken> entry : op.getData().getTokens().entrySet()) {
                if (entry.getValue().isSet()) {
                    tokenMap.put(entry.getKey().toUpperCase(), entry.getValue().toString());
                } else {
                    tokenMap.put(entry.getKey().toUpperCase(), null);
                }
            }

            for (String tokenName : configurationForTable.get().getTokenNames()) {
                jsonGenerator.writeStartObject();
                jsonGenerator.writeStringField(JsonFields.JSON_COLUMN_NAME, tokenName.toUpperCase());
                writeNullableField(JsonFields.JSON_VALUE, tokenMap.get(tokenName), jsonGenerator);
                jsonGenerator.writeEndObject();
            }
        }

        jsonGenerator.writeEndArray();
    }

    private void writeColumnArray(Op op, JsonGenerator jsonGenerator, DsOperation.OpType operationType) throws IOException {
        jsonGenerator.writeArrayFieldStart(JsonFields.JSON_COLUMNS);

        for (Col column : op) {
            writeColumnObject(jsonGenerator, operationType, column);
        }

        jsonGenerator.writeEndArray();
    }

    private void writeColumnObject(JsonGenerator jsonGenerator, DsOperation.OpType operationType, Col column) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(JsonFields.JSON_COLUMN_NAME, column.getName());

        if (operationType.isInsert()) {
            writeNullableField(JsonFields.JSON_VALUE, column.getAfterValue(), jsonGenerator);
        } else if (operationType.isDelete()) {
            writeNullableField(JsonFields.JSON_VALUE, column.getBeforeValue(), jsonGenerator);
        } else {
            writeUpdatedColumn(jsonGenerator, column);
        }

        jsonGenerator.writeEndObject();
    }

    private void writeUpdatedColumn(JsonGenerator jsonGenerator, Col column) throws IOException {
        writeNullableField(JsonFields.JSON_COLUMN_BEFORE, column.getBeforeValue(), jsonGenerator);
        writeNullableField(JsonFields.JSON_COLUMN_AFTER, column.getAfterValue(), jsonGenerator);

        if (column.hasAfterValue() && column.getAfter().isValueNull() && !column.hasBeforeValue()) {
            jsonGenerator.writeBooleanField(JsonFields.JSON_COLUMN_CHANGED, false);
        } else {
            jsonGenerator.writeBooleanField(JsonFields.JSON_COLUMN_CHANGED, column.isChanged());
        }
    }

    private void writeNullableField(String key, String value, JsonGenerator jsonGenerator) throws IOException {
        if (!isNull(value)) {
            jsonGenerator.writeStringField(key, value);
        } else {
            jsonGenerator.writeNullField(key);
        }
    }

    private boolean isNull(String value) {
        return StringUtils.isEmpty(value) || ORACLE_NULL.equalsIgnoreCase(value);
    }

}
