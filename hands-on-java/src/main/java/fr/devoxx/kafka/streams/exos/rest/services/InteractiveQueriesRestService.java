package fr.devoxx.kafka.streams.exos.rest.services;

import fr.devoxx.kafka.streams.exos.rest.InteractiveQueries;
import fr.devoxx.kafka.streams.exos.rest.utils.HostStoreInfo;
import fr.devoxx.kafka.streams.exos.rest.utils.KeyValueBean;
import fr.devoxx.kafka.streams.exos.transformations.statefull.CountNbrCommitByUser;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple REST proxy that runs embedded in the {@link InteractiveQueries}. This is used to
 * demonstrate how a developer can use the Interactive Queries APIs exposed by Kafka Streams to
 * locate and query the State Stores within a Kafka Streams Application.
 */
@Path("state")
public class InteractiveQueriesRestService {

    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private Server jettyServer;

    public InteractiveQueriesRestService(final KafkaStreams streams) {
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
    }


    @GET
    @Path("/contributors")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValueBean> CountNbrCommitByUser() {


        String storeName = CountNbrCommitByUser.NAME;

        final ReadOnlyKeyValueStore<String, Long> store = streams.store(storeName,
                QueryableStoreTypes.<String, Long>keyValueStore());
        if (store == null) {
            throw new NotFoundException();
        }

        //START EXO

        KeyValueIterator<String, Long> results = store.all();
        final List<KeyValueBean> keyValueBeans = new ArrayList<>();
        while (results.hasNext()) {
            final KeyValue<String,Long> next = results.next();
            keyValueBeans.add(new KeyValueBean( next.key, next.value));
        }

        //STOP EXO

        return keyValueBeans;

    }



    /**
     * Start an embedded Jetty Server on the given port
     *
     * @param port port to run the Server on
     * @throws Exception
     */
    public void start(final int port) throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(port);
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer.start();
    }

    /**
     * Stop the Jetty Server
     *
     * @throws Exception
     */
   public  void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

    @GET()
    @Path("/instances")
    @Produces(MediaType.APPLICATION_JSON)
    public List<HostStoreInfo> streamsMetadata() {
        return metadataService.streamsMetadata();
    }



}
