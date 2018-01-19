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