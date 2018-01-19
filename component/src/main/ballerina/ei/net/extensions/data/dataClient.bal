package ei.net.extensions.data;

import ballerina.data.sql;
import ballerina.net.http;
import ballerina.log;

// special characters
const string SPACE = " ";
const string OPEN_BRACKET = "(";
const string CLOSE_BRACKET = ")";
const string Q_MARK = "?";
const string EQUAL_SIGN = "=";
const string COMMA = ",";
const string EMPTY_STRING = "";
const string ASTERISK = "*";

// sql terms
const string INSERT_INTO = "INSERT INTO";
const string VALUES = "VALUES";
const string SELECT = "SELECT";
const string DELETE = "DELETE";
const string UPDATE = "UPDATE";
const string WHERE = "WHERE";
const string FROM = "FROM";
const string SET = "SET";
const string SHOW = "SHOW";
const string COLUMNS = "COLUMNS";

// http query params
const string HTTP_SELECT_PARAM = "$select";
const string HTTP_FILTER_PARAM = "$filter";

// CRUD type to identify the SQL mapping
enum CRUD_TYPE {
    CREATE, READ, UPDATE, DELETE
}


// Client connector to hold SQL Connector and expose the Http - CRUD actions
// SQL connector will get initialized when the Data Client connector creates. At the init all the table info will be
// retrieved from the Database and cached
public connector DataClient (sql:DB dbType, string hostOrPath, int port, string dbName, string username, string password,
                             string tableName, sql:ConnectionProperties options) {

    // underlying SQL connector wrapped by this connector
    sql:ClientConnector sqlClient = create sql:ClientConnector(dbType, hostOrPath, port, dbName, username, password, options);

    // map to hold table information. It will contain tuples of <column-name, data-type>
    map columnInfo = loadTableData(sqlClient, tableName);

    // action:PATCH
    // This action will get mapped into UPDATE CRUD options of the database
    action patch(http:Request req) (http:Response) {
        endpoint<sql:ClientConnector> sqlEp {
            sqlClient;
        }
        // response to be sent to the invoker
        http:Response res = {};
        res.statusCode = 200;

        // initialized string to hold the contents of the where clause
        string filterExpressions = EMPTY_STRING;

        // 1. process the json payload and generate a parametrized string and sql parameter struct array
        var columnNamesText, _, params = processJsonInput(req.getJsonPayload(), columnInfo, CRUD_TYPE.UPDATE);

        // 2. process the http query params and generate 'where' expression
        // get http query params
        map queryParams = req.getQueryParams();
        // extract query params for where clause
        sql:Parameter[] filterParams = [];
        if (queryParams[HTTP_FILTER_PARAM] != null) {
            filterExpressions,_ = (string)queryParams[HTTP_FILTER_PARAM];
            filterExpressions,filterParams = formatFilterExpression(filterExpressions, columnInfo);
        }

        // append filterParams and bodyParams into single Parameter array
        if (lengthof filterParams > 0) {
            int i = 0;
            int filterParamsSize = lengthof filterParams;
            while (i < filterParamsSize) {
                params[lengthof params] = filterParams[i];
                i = i + 1;
            }
        }

        try {
            // execute the sql query
            string parametrizedQuery = UPDATE + SPACE + tableName + SPACE + SET + SPACE + columnNamesText + filterExpressions;
            log:printTrace("execute sql: " + parametrizedQuery);
            int value = sqlEp.update(parametrizedQuery, params);
            // set sql response to the http:Response
            res.setJsonPayload({rows:value});
        } catch (error e) {
            // handle the error
            handleSQLError("error while excuting UPDATE statement", e, res);
        }
        return res;
    }

    // action:DELETE
    // This action will get mapped into DELETE CRUD options of the database
    action delete(http:Request req) (http:Response) {
        endpoint<sql:ClientConnector> sqlEp {
            sqlClient;
        }
        // response to be sent to the invoker
        http:Response res = {};
        res.statusCode = 200;

        // initialized string to hold the contents of the where clause
        string filterExpressions = EMPTY_STRING;
        sql:Parameter[] params;

        // process the http query params and generate 'where' expression
        map queryParams = req.getQueryParams();
        // extract query params for where clause
        if (queryParams[HTTP_FILTER_PARAM] != null) {
            filterExpressions,_ = (string)queryParams[HTTP_FILTER_PARAM];
            filterExpressions,params = formatFilterExpression(filterExpressions, columnInfo);
        }

        try {
            // execute the sql query
            string parametrizedQuery = DELETE + SPACE + FROM + SPACE + tableName + filterExpressions;
            log:printTrace("execute sql: " + parametrizedQuery);
            int value = sqlEp.update(parametrizedQuery, params);
            // set sql response to the http:Response
            res.setJsonPayload({rows:value});
        } catch (error e) {
            // handle the error
            handleSQLError("error while excuting DELETE statement", e, res);
        }
        return res;
    }

    // action:GET
    // This action will get mapped into READ CRUD option of the database
    action get(http:Request req) (http:Response) {
        endpoint<sql:ClientConnector> sqlEp {
            sqlClient;
        }
        // response to be sent to the invoker
        http:Response res = {};
        res.statusCode = 200;

        // initialized string to hold the contents of the select clause
        string selectColumns = ASTERISK;
        // initialized string to hold the contents of the where clause
        string filterExpressions = EMPTY_STRING;
        sql:Parameter[] params;

        // process the http query params and generate 'where' expression
        // extract query params for select clause
        map queryParams = req.getQueryParams();
        if (queryParams[HTTP_SELECT_PARAM] != null) {
            selectColumns, _ = (string)queryParams[HTTP_SELECT_PARAM];
        }
        // extract query params for where clause
        if (queryParams[HTTP_FILTER_PARAM] != null) {
            filterExpressions,_ = (string)queryParams[HTTP_FILTER_PARAM];
            filterExpressions,params = formatFilterExpression(filterExpressions, columnInfo);
        }

        try {
            // execute the sql query
            string parametrizedQuery = SELECT + SPACE + selectColumns + SPACE + FROM + SPACE + tableName + filterExpressions;
            log:printTrace("execute sql: " + parametrizedQuery);
            datatable dt = sqlEp.select(parametrizedQuery, params, null);
            // set sql response to the http:Response
            var jsonPayload, e = <json>dt;
            res.setJsonPayload(jsonPayload);
        } catch (error e) {
            // handle the error
            handleSQLError("error while excuting SELECT statement", e, res);
        }
        return res;
    }

    // action:POST
    // This action will get mapped into CREATE CRUD option of the database
    action post (http:Request req) (http:Response) {
        endpoint<sql:ClientConnector> sqlEp {
            sqlClient;
        }
        // response to be sent to the invoker
        http:Response res = {};
        res.statusCode = 200;

        // process the json payload and generate a parametrized string and sql parameter struct array
        var columnNamesText, argumentsText, params = processJsonInput(req.getJsonPayload(), columnInfo, CRUD_TYPE.READ);

        try {
            // execute the sql query
            string parametrizedQuery = INSERT_INTO + SPACE + tableName + SPACE + OPEN_BRACKET + columnNamesText + CLOSE_BRACKET + SPACE + VALUES + SPACE + OPEN_BRACKET + argumentsText + CLOSE_BRACKET;
            log:printTrace("execute sql: " + parametrizedQuery);
            int value = sqlEp.update(parametrizedQuery, params);
            // set sql response to the http:Response
            res.setJsonPayload({rows:value});
        } catch (error e) {
            // handle the error
            handleSQLError("error while excuting INSERT statement", e, res);
        }

        return res;
    }
}

@Description { value:"  convert http query param string to sql expression to be used with where
                        sample input \"name eq 'bob' and age eq 22\"
                        sample output \" WHERE name='bob' and age=22\", [{type:VARCHAR, value:\"bob\"},{type:INT, value:22}]"}
@Param { value:"filter: http query string" }
@Param { value:"columnInfo: map containing info about the table" }
@Return { value:"string: generated SQL parameterized where string" }
@Return { value:"parameter[]: values as SQL parameters" }
function formatFilterExpression(string filter, map columnInfo) (string, sql:Parameter[]) {
    // todo:add other regex
    filter,_ = filter.replaceAllWithRegex({pattern:"eq"},"=");
    filter,_ = filter.replaceAllWithRegex({pattern:"gt"},">");
    filter,_ = filter.replaceAllWithRegex({pattern:"lt"},"<");
    filter,_ = filter.replaceAllWithRegex({pattern:"not"},"<>");

    var splittedFilter,_ = filter.findAllWithRegex({pattern:"([^\']\\S*|\'.+?\')\\s*"});

    string outputFilterText = EMPTY_STRING;
    sql:Parameter[] params = [];

    int i = 0;
    int splittedFilterSize = lengthof splittedFilter;
    while(i < splittedFilterSize) {
        string columnName = splittedFilter[i].trim();

        if (columnInfo[columnName] == null) {
            error e = {msg:"invalid column name in the filter condition: " + columnName};
            throw e;
        }
        outputFilterText = outputFilterText + SPACE + columnName + SPACE + splittedFilter[i+1].trim() + SPACE + Q_MARK;
        string value = splittedFilter[i+2].replaceAll("\'",EMPTY_STRING);
        if (i+3 < splittedFilterSize) {
            outputFilterText =  outputFilterText + SPACE + splittedFilter[i+3].trim();
        }
        var elementTypeText,_= (string) columnInfo[columnName];
        sql:Parameter param = getSqlParameter(value, elementTypeText);
        params[lengthof params] = param;
        i = i + 4;
    }

    outputFilterText = SPACE + WHERE + SPACE + outputFilterText;

    return outputFilterText, params;
}

@Description { value:" Process Json object into a SQL string. Convertion will be done according to the SQL operation its going to invoke"}
@Param { value:"jsonPayload: json payload to be processed" }
@Param { value:"columnInfo: map containing info about the table" }
@Param { value:"crudType: SQL operation which this will be used" }
@Return { value:"string: generated SQL (parameterized) string" }
@Return { value:"string: equal number Q_MARKS to be embed with the SQL query" }
@Return { value:"parameter[]: values as SQL parameters" }
function processJsonInput (json jsonPayload, map columnInfo, CRUD_TYPE crudType) (string, string, sql:Parameter[]) {
    string columns = EMPTY_STRING;
    string arguments = EMPTY_STRING;
    sql:Parameter[] params = [];
    string[] columnNames = columnInfo.keys();

    int columnCount = lengthof columnNames;
    int i = 0;

    while (i < columnCount) {
        string columnName = columnNames[i];

        json elementValue = jsonPayload[columnName];
        if (elementValue == null) {
            i = i + 1;
            next;
        }

        // create the string
        // this is not the first, therefore append delimiters
        if ((lengthof params) != 0) {
            columns = columns + COMMA;
            arguments = arguments + COMMA;
        }
        columns = columns + columnName;
        if (crudType == CRUD_TYPE.UPDATE) {
            columns = columns + SPACE + EQUAL_SIGN + SPACE + Q_MARK;
        }
        arguments = arguments + Q_MARK;

        // create the param object
        var elementTypeText,_= (string) columnInfo[columnName];
        sql:Parameter param = getSqlParameter(elementValue, elementTypeText);
        params[lengthof params] = param;

        i = i + 1;
    }
    return columns, arguments, params;
}


@Description { value:" Create SQL parameter object for any given value, type pair"}
function getSqlParameter (any fieldValue, string sqlTypeText) (sql:Parameter) {
    sql:Parameter param = {};

    if (sqlTypeText.equalsIgnoreCase("CHAR")) {

    } else if (sqlTypeText.equalsIgnoreCase("LONGVARCHAR")) {

    } else if (sqlTypeText.equalsIgnoreCase("NCHAR")) {

    } else if (sqlTypeText.equalsIgnoreCase("LONGNVARCHAR")) {

    } else if (sqlTypeText.equalsIgnoreCase("NVARCHAR")) {

    } else if (sqlTypeText.equalsIgnoreCase("BIT")) {

    } else if (sqlTypeText.equalsIgnoreCase("BOOLEAN")) {
        param = {sqlType:sql:Type.BOOLEAN, value:fieldValue};
    } else if (sqlTypeText.equalsIgnoreCase("TINYINT")) {

    } else if (sqlTypeText.equalsIgnoreCase("SMALLINT")) {

    } else if (sqlTypeText.equalsIgnoreCase("INTEGER") || sqlTypeText.equalsIgnoreCase("INT")) {
        param = {sqlType:sql:Type.INTEGER, value:fieldValue};
    } else if (sqlTypeText.equalsIgnoreCase("BIGINT")) {

    } else if (sqlTypeText.equalsIgnoreCase("NUMERIC")) {

    } else if (sqlTypeText.equalsIgnoreCase("DECIMAL")) {

    } else if (sqlTypeText.equalsIgnoreCase("REAL")) {

    } else if (sqlTypeText.equalsIgnoreCase("FLOAT")) {

    } else if (sqlTypeText.equalsIgnoreCase("DOUBLE")) {

    } else if (sqlTypeText.equalsIgnoreCase("BINARY")) {

    } else if (sqlTypeText.equalsIgnoreCase("BLOB")) {

    } else if (sqlTypeText.equalsIgnoreCase("LONGVARBINARY")) {

    } else if (sqlTypeText.equalsIgnoreCase("VARBINARY")) {

    } else if (sqlTypeText.equalsIgnoreCase("CLOB")) {

    } else if (sqlTypeText.equalsIgnoreCase("NCLOB")) {

    } else if (sqlTypeText.equalsIgnoreCase("DATE")) {

    } else if (sqlTypeText.equalsIgnoreCase("TIME")) {

    } else if (sqlTypeText.equalsIgnoreCase("DATETIME")) {

    } else if (sqlTypeText.equalsIgnoreCase("TIMESTAMP")) {

    } else if (sqlTypeText.equalsIgnoreCase("ARRAY")) {

    } else if (sqlTypeText.equalsIgnoreCase("STRUCT")) {

    } else if (sqlTypeText.equalsIgnoreCase("VARCHAR"))  {
        param = {sqlType:sql:Type.VARCHAR, value:fieldValue};
    } else {
        error e = {msg: "unknown sql type " + sqlTypeText};
        throw e;
    }
    return param;
}

@Description { value:" Load the table information into a map. This will be only called when initializing the connector"}
function loadTableData (sql:ClientConnector sqlClient, string tableName) (map) {
    endpoint<sql:ClientConnector> sqlEp {
        sqlClient;
    }

    map columnInfo = {};
    datatable dt;
    // get table metadata
    try {
        dt = sqlEp.select(SHOW + SPACE + COLUMNS + SPACE + FROM + SPACE + tableName, null, null);
    } catch (error e) {
        // logged the error because this function is called when initializing the Connector, therefore we can't handle it from the top
        log:printError("error getting data from table " + e.msg);
        throw e;
    }

    // convert metadata to a json object
    var jsonPayload, e = <json>dt;
    if (e != null) {
        log:printError("error processing column meta data " + e.msg);
        throw e;
    }

    // fill the map with table data. map will contain <field_name, field_type> tuples
    int i = 0;
    int numberOfColumns = lengthof jsonPayload;
    // todo: use foreach instead of while loop
    while (i < numberOfColumns) {
        var fieldName, er1 = (string)jsonPayload[i].Field;
        if (er1 != null) {
            log:printError("error reading field name " + e.msg);
            throw e;
        }
        var fieldType, er2 = (string)jsonPayload[i].Type;
        if (er2 != null) {
            log:printError("error reading field type " + e.msg);
            throw e;
        }
        columnInfo[fieldName] = fieldType.split("\\(")[0];
        i = i + 1;
    }
    log:printTrace("retrieved column information to create data client from table " + tableName);
    return columnInfo;
}


@Description { value:" Function to handle sql invocation errors. This will log the error and embed the error message into http:Response"}
function handleSQLError(string message, error e, http:Response response) {
    string errorMessage = message + SPACE + e.msg;
    response.statusCode = 400;
    response.setJsonPayload({error:errorMessage});
    log:printError(errorMessage);
}