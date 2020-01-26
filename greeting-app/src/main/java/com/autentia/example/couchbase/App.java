package com.autentia.example.couchbase;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public static void main(final String[] args) throws IOException {
        System.out.println("Comenzamos");

        crearLog();
        final Cluster cluster = conexion();
        final Bucket bucket = conectarABucket(cluster);

        // JsonObject hello = crearObjetoInsertarEnBucket();
        // insertarObjetoBucket(bucket, hello);

        //crearBackup(bucket);
        //crearNewLocal(bucket);
        definitivo(bucket);

        // crearIndiceBucket(bucket);

        // consultar(bucket);

        desconectar(cluster, bucket);
    }

    private static void definitivo(Bucket bucket) throws IOException {
        // imprime el contenido del documento completo, si existe dentro de ese bucket
        // System.out.println("DOCUMENTO_NUEVO: "+bucket.get("g:hello2").content());
        // String v = "TER";
        // System.out.println("DOCUMENTO_NUEVO: "+bucket.get("g:hello2").content());
        List<String> lista = new ArrayList<>();
        lista.add("g:hello");
        lista.add("g:hello2");
        int i = 0;
        for (String e : lista) {
            System.out.println("DOCUMENTO_NUEVO: " + bucket.get(e).content());
            FileWriter f;

           Map<String,Object> submapa = new HashMap<String,Object>();
           submapa.put("sub1", "subvalue1");
           Map<String, Object> mapa = new HashMap<String, Object>();
           mapa.put("key_1", "value_1");
           mapa.put("key_2", "value_2");
           mapa.put("SubKEY",submapa);
              //  f = new FileWriter("backup_" + i);
                f = new FileWriter("D:\\PROYECTOS\\couchbase\\couchDB_java\\greeting-app\\src\\main\\java\\com\\autentia\\example\\couchbase\\update_" + i+".json");
                JsonObject contenido = JsonObject.empty()
                .put("atributo_1", "clave_1")
                .put("atributo_2","clave_2")
                .put("objeto",JsonObject.from(mapa));
                f.write(contenido.toString());
                bucket.upsert(JsonDocument.create("g:hello"+i, contenido));
                //f.write(bucket.get(e).content().toString());
         
                i++;
                f.close();
        
        }
    }

    private static void crearBackup(Bucket bucket) throws IOException {
        // imprime el contenido del documento completo, si existe dentro de ese bucket
        // System.out.println("DOCUMENTO_NUEVO: "+bucket.get("g:hello2").content());
        // String v = "TER";
        // System.out.println("DOCUMENTO_NUEVO: "+bucket.get("g:hello2").content());
        List<String> lista = new ArrayList<>();
        lista.add("g:hello");
        lista.add("g:hello2");
        int i = 0;
        for (String e : lista) {
            System.out.println("DOCUMENTO_NUEVO: " + bucket.get(e).content());
            FileWriter f;
           
              //  f = new FileWriter("backup_" + i);
                f = new FileWriter("D:\\PROYECTOS\\couchbase\\couchDB_java\\greeting-app\\src\\main\\java\\com\\autentia\\example\\couchbase\\backup_" + i+".json");
                f.write(bucket.get(e).content().toString());
                i++;
                f.close();
        
        }
    }
    private static void crearNewLocal(Bucket bucket) throws IOException {
        // imprime el contenido del documento completo, si existe dentro de ese bucket
        // System.out.println("DOCUMENTO_NUEVO: "+bucket.get("g:hello2").content());
        // String v = "TER";
        // System.out.println("DOCUMENTO_NUEVO: "+bucket.get("g:hello2").content());
        List<String> lista = new ArrayList<>();
        lista.add("g:hello");
        lista.add("g:hello2");
        int i = 0;
        for (String e : lista) {
            System.out.println("DOCUMENTO_NUEVO: " + bucket.get(e).content());
            FileWriter f;

           Map<String,Object> submapa = new HashMap<String,Object>();
           submapa.put("sub1", "subvalue1");
           Map<String, Object> mapa = new HashMap<String, Object>();
           mapa.put("key_1", "value_1");
           mapa.put("key_2", "value_2");
           mapa.put("SubKEY",submapa);
              //  f = new FileWriter("backup_" + i);
                f = new FileWriter("D:\\PROYECTOS\\couchbase\\couchDB_java\\greeting-app\\src\\main\\java\\com\\autentia\\example\\couchbase\\update_" + i+".json");
                JsonObject contenido = JsonObject.empty()
                .put("atributo_1", "clave_1")
                .put("atributo_2","clave_2")
                .put("objeto",JsonObject.from(mapa));
                f.write(contenido.toString());
                bucket.upsert(JsonDocument.create("g:hello"+i, contenido));
                //f.write(bucket.get(e).content().toString());
         
                i++;
                f.close();
        
        }
    }

    /**
     * Si existe el documento, lo machaca, sino lo crea
     * 
     * @param bucket
     * @param hello
     */
    private static void insertarObjetoBucket(final Bucket bucket, final JsonObject hello) {
        // bucket.upsert(JsonDocument.create("g:hello2", hello));
        bucket.upsert(JsonDocument.create("g:hello2", hello));
    }

    private static JsonObject crearObjetoInsertarEnBucket() {
        // creamos una estructura de tipo json, que es lo que almacena couch
        final JsonObject hello = JsonObject.create().put("Cuerpo_1", "sera lista_X").put("cuerpo_2", "antonio");
        return hello;
    }

    private static Bucket conectarABucket(final Cluster cluster) {
        // schema en el que hacer los cambios
        final Bucket bucket = cluster.openBucket("greeting");
        return bucket;
    }

    private static void crearIndiceBucket(final Bucket bucket) {
        // obtenemos permisos en el bucket, para crear en este caso un índice
        bucket.bucketManager().createN1qlPrimaryIndex(true, false);
    }

    private static void consultar(final Bucket bucket) {
        // consulta para buscar los documentos en el bucket `greeting`, donde la key:
        // autor = foo
        final N1qlQueryResult result = bucket
                // .query(N1qlQuery.parameterized("SELECT message FROM `greeting` WHERE
                // author=$1", JsonArray.from("foo")));
                .query(N1qlQuery.parameterized("SELECT meta.id FROM `greeting` WHERE author=$1",
                        JsonArray.from("foo")));
        for (final N1qlQueryRow row : result) {
            System.out.println("FILA: " + row);
        }
    }

    private static void desconectar(final Cluster cluster, final Bucket bucket) {
        // desconectamos del bucket `greeting` primero
        bucket.close();
        // desconectamos del cluster, conexión principal.
        cluster.disconnect();
    }

    private static Cluster conexion() {
        // this tunes the SDK (to customize connection timeout)
        // aumentamos el tiempo para conectar, por defecto 5 segundos, no me da tiempo y
        // me echa
        final CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().connectTimeout(10000) // 10000ms = 10s,
                                                                                                     // default is 5s
                .build();
        // conectamos con el cluster(1 o + nodos(lo normal 1 nodo por servidor))
        // contiene los buckets ~ schemas Mysql
        final Cluster cluster = CouchbaseCluster.create(env, "172.18.241.234:8091");
        // login de usuario de acceso
        cluster.authenticate("Administrator", "123456");
        return cluster;
    }

    private static void crearLog() {
        final Properties systemProperties = System.getProperties();
        // añadimos a las propiedades del sistema los paquetes que van a soltar log
        systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
        System.setProperties(systemProperties);
        // creamos una instancia del paquete que va a crear los log
        final Logger logger = Logger.getLogger("com.couchbase.client");
        // configuramos para que esos log no se muestren por consola
        logger.setLevel(java.util.logging.Level.OFF);
    }


}
