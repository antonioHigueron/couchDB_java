package com.autentia.example.couchbase;

import java.util.Properties;
import java.util.logging.Logger;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Comenzamos");
        Properties systemProperties = System.getProperties();
        //añadimos a las propiedades del sistema los paquetes que van a soltar log
        systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
        System.setProperties(systemProperties);
        //creamos una instancia del paquete que va a crear los log
        Logger logger = Logger.getLogger("com.couchbase.client");
        // configuramos para que esos log no se muestren por consola
        logger.setLevel(java.util.logging.Level.OFF);
        //this tunes the SDK (to customize connection timeout)
        //aumentamos el tiempo para conectar, por defecto 5 segundos, no me da tiempo y me echa
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
        .connectTimeout(10000) //10000ms = 10s, default is 5s
        .build();
        //conectamos con el cluster(1 o + nodos(lo normal 1 nodo por servidor)) contiene los buckets ~ schemas Mysql
        Cluster cluster = CouchbaseCluster.create(env,"172.18.247.226:8091");
        // login de usuario de acceso
        cluster.authenticate("Administrator", "123456");
        // schema en el que hacer los cambios
        Bucket bucket = cluster.openBucket("greeting");
        // creamos una estructura de tipo json, que es lo que almacena couch
        JsonObject hello = JsonObject.create().put("parámetro_1", "Hola Java!").put("mi nombre", "antonio");
        //bucket.upsert(JsonDocument.create("g:hello2", hello));
        bucket.upsert(JsonDocument.create("g:hello2", hello));
        //imprime el contenido del documento completo, si existe dentro de ese bucket
        System.out.println("DOCUMENTO_NUEVO: "+bucket.get("g:hello2"));
        //obtenemos permisos en el bucket, para crear en este caso un índice
        bucket.bucketManager().createN1qlPrimaryIndex(true, false);
        //consulta para buscar los documentos en el bucket `greeting`, donde la key:  autor = foo
        N1qlQueryResult result = bucket
                //.query(N1qlQuery.parameterized("SELECT message FROM `greeting` WHERE author=$1", JsonArray.from("foo")));
                .query(N1qlQuery.parameterized("SELECT meta.id FROM `greeting` WHERE author=$1", JsonArray.from("foo")));

        for (N1qlQueryRow row : result) {
            System.out.println("FILA: "+row);
        }
        //desconectamos del bucket `greeting` primero
        bucket.close();
        //desconectamos del cluster, conexión principal.
        cluster.disconnect();
    }
}
