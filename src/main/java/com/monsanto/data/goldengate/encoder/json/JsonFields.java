package com.monsanto.data.goldengate.encoder.json;

public interface JsonFields {
    String JSON_COLUMNS = "columns";
    String JSON_TOKENS = "tokens";

    String JSON_FILESEQNO = "seqNo";
    String JSON_FILERBA = "rba";
    String JSON_TRANSIND = "transInd";

    String JSON_DBUSER = "dbUser";
    String JSON_GGHOST = "ggHost";
    String JSON_TRANSID = "transId";

    String JSON_COLUMN_NAME = "name";
    String JSON_COLUMN_BEFORE = "before";
    String JSON_COLUMN_AFTER = "after";
    String JSON_COLUMN_CHANGED = "changed";
    String JSON_SCHEMA = "schema";
    String JSON_TABLE = "table";
    String JSON_TIMESTAMP = "tsp";
    String JSON_TIMEZONE = "tmZone";
    String JSON_MODTYPE = "modType";
    String JSON_VALUE = "value";
}
