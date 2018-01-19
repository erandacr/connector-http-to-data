# EI Http to Data Extension

Sample Configuration

```ballerina
import ei.net.extensions.data;
import ballerina.net.http;
import ballerina.data.sql;


@http:configuration {
    basePath:"/hello"
}
service<http> sampleService {
    endpoint<data:DataClient> dataEp {
        create data:DataClient(sql:DB.MYSQL, "localhost", 3306, "testdb", "root", "root", "person", {maximumPoolSize:5});
    }

    @http:resourceConfig {
        methods:["POST"],
        path:"/helloTable"
    }
    resource dbPost (http:Request req, http:Response res) {
        http:Response response = dataEp.post(req);
        _=res.forward(response);

    }

    @http:resourceConfig {
        methods:["GET"],
        path:"/helloTable"
    }
    resource dbGet (http:Request req, http:Response res) {
        http:Response response = dataEp.get(req);
        _=res.forward(response);

    }

    @http:resourceConfig {
        methods:["DELETE"],
        path:"/helloTable"
    }
    resource dbDelete (http:Request req, http:Response res) {
        http:Response response = dataEp.delete(req);
        _=res.forward(response);

    }

    @http:resourceConfig {
        methods:["PATCH"],
        path:"/helloTable"
    }
    resource dbPatch (http:Request req, http:Response res) {
        http:Response response = dataEp.patch(req);
        _=res.forward(response);

    }

}
```

Example scenario.

1. Create a table as follows in the database,

````
CREATE TABLE `person` (
  `name` VARCHAR(255) NOT NULL,
  `age` INT NULL,
  `NIC` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`NIC`));
````

2. Deploy http-to-data connector into Ballerina runtime

3. Run Ballerina with above sample bal source

4. Use following commands to manipulate data through http interface

CREATE
````
curl -X POST \
  http://localhost:9090/hello/helloTable \
  -H 'content-type: application/json' \
  -d '{
   "name":"bob",
   "age": 22,
   "NIC": "12345"
}'
````

READ
````
curl -X GET \
  'http://localhost:9090/hello/helloTable?%24select=name%2Cage&%24filter=name%20eq%20'\''bob'\''%20and%20age%20eq%2028'
````

UPDATE
````
curl -X PATCH \
  'http://localhost:9090/hello/helloTable?%24filter=name%20eq%20'\''bob'\''' \
  -H 'content-type: application/json' \
  -d '{
   "age": 28
}'
````

DELETE
````
curl -X DELETE \
  'http://localhost:9090/hello/helloTable?%24filter=name%20eq%20'\''bob'\''%20and%20age%20eq%2022'
````