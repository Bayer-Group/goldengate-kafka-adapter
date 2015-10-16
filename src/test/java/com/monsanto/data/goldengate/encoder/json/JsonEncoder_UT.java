package com.monsanto.data.goldengate.encoder.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.goldengate.atg.datasource.*;
import com.goldengate.atg.datasource.adapt.Col;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableName;
import com.google.common.base.Optional;
import com.monsanto.data.goldengate.config.Conf;
import com.monsanto.data.goldengate.config.TableConfiguration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Op.class})
public class JsonEncoder_UT {
    @Mock
    private Conf configuration;
    @Mock
    private DsMetaData metaData;
    @Mock
    private DsTransaction transaction;
    @Mock
    private Tx tx;
    @Mock
    private TableName tableName;
    @Mock
    private DsRecord record;
    @Mock
    private Op op;

    private JsonEncoder jsonEncoder;

    private static final String SCHEMA_NAME = "foo";
    private static final String TABLE_NAME = "bar";
    private static final String OP_TIMESTAMP = "2015-05-01 23:59:01.999";

    private final JsonFactory jsonFactory = new JsonFactory();
    private ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
    private JsonNode message;

    @Before
    public void setUp() {
        jsonEncoder = new JsonEncoder(configuration);
        message = null;

        when(configuration.getTableConfiguration(anyString(),anyString())).thenReturn(Optional.<TableConfiguration>absent());

        trainSimpleOpMock();
    }

    @Test
    public void writesTableNameAttributes() throws Exception {
        encodeMessage();

        assertThat(readJsonTextAttribute(JsonFields.JSON_SCHEMA), equalTo(SCHEMA_NAME));
        assertThat(readJsonTextAttribute(JsonFields.JSON_TABLE), equalTo(TABLE_NAME));
    }

    @Test
    public void writesOperationAttributes() throws Exception {
        when(op.getTimestamp()).thenReturn(OP_TIMESTAMP);
        when(op.getOperationType()).thenReturn(DsOperation.OpType.DO_INSERT);

        encodeMessage();

        assertThat(readJsonTextAttribute(JsonFields.JSON_TIMESTAMP), equalTo(OP_TIMESTAMP));
        assertThat(readJsonTextAttribute(JsonFields.JSON_MODTYPE), equalTo("I"));
    }

    @Test
    public void writesTransactionMetadataAttributes() throws Exception {

    }

    @Test
    public void columns_writesColumnNameAttribute() throws Exception {
        String expectedColumnName = "fizz";
        String expectedColumnAfterValue = "buzz";

        Col column1 = createMockColumn(expectedColumnName, null, expectedColumnAfterValue);

        when(op.getOpType()).thenReturn(DsOperation.OpType.DO_INSERT);
        List<Col> columns = Collections.singletonList(column1);
        when(op.iterator()).thenReturn(columns.listIterator());

        encodeMessage();

        ObjectNode columnObject = (ObjectNode) getOnlyColumnObject();

        String serializedName = readTextAttributeFromObject(columnObject, JsonFields.JSON_COLUMN_NAME);
        String serializedAfterValue = readTextAttributeFromObject(columnObject, JsonFields.JSON_VALUE);

        assertThat(serializedName, is(expectedColumnName));
        assertThat(serializedAfterValue, is(expectedColumnAfterValue));
    }

    @Test
    public void columns_inserts_writesAttributeAfterImage() throws Exception {
        String expectedColumnAfterValue = "buzz";

        Col column1 = createMockColumn("foo", null, expectedColumnAfterValue);

        when(op.getOpType()).thenReturn(DsOperation.OpType.DO_INSERT);
        List<Col> columns = Collections.singletonList(column1);
        when(op.iterator()).thenReturn(columns.listIterator());

        encodeMessage();

        ObjectNode columnObject = (ObjectNode) getOnlyColumnObject();

        String serializedAfterValue = readTextAttributeFromObject(columnObject, JsonFields.JSON_VALUE);

        assertThat(serializedAfterValue, is(expectedColumnAfterValue));
    }

    @Test
    public void columns_deletes_writesAttributeBeforeImage() throws Exception {
        String expectedColumnName = "fizz";
        String expectedColumnBeforeValue = "buzz";

        Col column1 = createMockColumn(expectedColumnName, expectedColumnBeforeValue, null);

        when(op.getOpType()).thenReturn(DsOperation.OpType.DO_DELETE);

        List<Col> columns = Collections.singletonList(column1);
        when(op.iterator()).thenReturn(columns.listIterator());

        encodeMessage();

        ObjectNode columnObject = (ObjectNode) getOnlyColumnObject();

        String serializedBeforeValue = readTextAttributeFromObject(columnObject, JsonFields.JSON_VALUE);

        assertThat(serializedBeforeValue, is(expectedColumnBeforeValue));
    }

    private JsonNode getColumnArrayHead(ArrayNode columnsArray) {
        JsonNode columnJsonNode = columnsArray.get(0);
        assertThat(columnJsonNode.getNodeType(), is(JsonNodeType.OBJECT));
        return columnJsonNode;
    }

    private JsonNode getOnlyColumnObject() {
        ArrayNode columnsArray = getColumnsArrayNode();
        assertThat(columnsArray.size(), equalTo(1));

        return getColumnArrayHead(columnsArray);
    }

    @Test
    public void columns_updates_writesBeforeAndAfterImage() throws Exception {
        String expectedColumnName = "fizz";
        String expectedColumnBeforeValue = "buzz";
        String expectedColumnAfterValue = "baz";

        Col column1 = createMockColumn(expectedColumnName, expectedColumnBeforeValue, expectedColumnAfterValue);

        List<Col> columns = Collections.singletonList(column1);
        when(op.iterator()).thenReturn(columns.listIterator());

        when(op.getOpType()).thenReturn(DsOperation.OpType.DO_UPDATE);

        encodeMessage();

        ObjectNode columnObject = (ObjectNode) getOnlyColumnObject();

        String serializedBeforeValue = readTextAttributeFromObject(columnObject, JsonFields.JSON_COLUMN_BEFORE);
        String serializedAfterValue = readTextAttributeFromObject(columnObject, JsonFields.JSON_COLUMN_AFTER);

        assertThat(serializedBeforeValue, is(expectedColumnBeforeValue));
        assertThat(serializedAfterValue, is(expectedColumnAfterValue));
    }

    @Test
    public void columns_updates_changeBooleanIsFalseForChangeFromNullToNull() throws Exception {
        String expectedColumnName = "fizz";
        String expectedColumnBeforeValue = "buzz";
        String expectedColumnAfterValue = "baz";

        Col column1 = createMockColumn(expectedColumnName, expectedColumnBeforeValue, expectedColumnAfterValue);

        when(column1.hasAfterValue()).thenReturn(true);
        when(column1.hasBeforeValue()).thenReturn(false);

        DsColumn mockDsColumn = mock(DsColumn.class);
        when(mockDsColumn.isValueNull()).thenReturn(true);
        when(column1.getAfter()).thenReturn(mockDsColumn);

        when(op.getOpType()).thenReturn(DsOperation.OpType.DO_UPDATE);

        List<Col> columns = Collections.singletonList(column1);
        when(op.iterator()).thenReturn(columns.listIterator());

        encodeMessage();

        ObjectNode columnObject = (ObjectNode) getOnlyColumnObject();

        boolean serializedChangedBoolean = columnObject.get(JsonFields.JSON_COLUMN_CHANGED).asBoolean(true);

        assertThat(serializedChangedBoolean, is(false));
    }

    private ArrayNode getColumnsArrayNode() {
        JsonNode jsonNode = message.get("columns");
        assertThat(jsonNode.isArray(), is(true));

        ArrayNode columnsArray = (ArrayNode) jsonNode;
        assertThat(columnsArray.size(), is(1));
        return columnsArray;
    }

    private String readTextAttributeFromObject(ObjectNode columnJsonNode, String objectFieldName) {
        return columnJsonNode.get(objectFieldName).asText();
    }

    private Col createMockColumn(String columnName, String columnBeforeValue, String columnAfterValue) {
        Col column1 = mock(Col.class);
        when(column1.getName()).thenReturn(columnName);
        when(column1.getBeforeValue()).thenReturn(columnBeforeValue);
        when(column1.getAfterValue()).thenReturn(columnAfterValue);
        return column1;
    }

    private String readJsonTextAttribute(String name) {
        return message.get(name).asText();
    }

    private void encodeMessage() throws IOException {
        byte[] encodedOperation = jsonEncoder.encode(tx, op);
        message = objectMapper.readTree(encodedOperation);
    }

    private void trainSimpleOpMock() {
        TableName tableName = mock(TableName.class);
        when(tableName.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(tableName.getShortName()).thenReturn(TABLE_NAME);
        when(op.getTableName()).thenReturn(tableName);

        when(op.getOperationType()).thenReturn(DsOperation.OpType.DO_INSERT);
        when(op.getRba()).thenReturn(1L);
        when(op.getTxState()).thenReturn(TxState.END);
        when(op.iterator()).thenReturn(Collections.EMPTY_LIST.listIterator());
    }
}
