package fr.devoxx.kafka.streams.exos.rest.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.devoxx.kafka.streams.exos.rest.InteractiveQueries;
import fr.devoxx.kafka.streams.exos.rest.utils.HostStoreInfo;
import fr.devoxx.kafka.streams.exos.rest.utils.KeyValueBean;
import fr.devoxx.kafka.streams.exos.store.WindowedLocalKVStore;
import fr.devoxx.kafka.streams.exos.transformations.statefull.CountNbrCommitByUser;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
  private String host;

  public InteractiveQueriesRestService(final KafkaStreams streams, String host) {
    this.streams = streams;
    this.metadataService = new MetadataService(streams);
    this.host = host;
  }


  @GET
  @Path("/keyval/{storeName}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<KeyValueBean> CountNbrCommitByUser(@PathParam("storeName") final String storeName) {


    final List<KeyValueBean> keyValueBeans = new ArrayList<>();

    streams.allMetadataForStore(storeName).forEach(metadata -> {

      String comppleteHost = metadata.host() + ":" + metadata.port();

      if (Objects.equals(comppleteHost, host)) {
        final ReadOnlyKeyValueStore<String, Long> store = streams.store(storeName, QueryableStoreTypes.<String, Long>keyValueStore());
        if (store == null) {
          throw new NotFoundException();
        }

        KeyValueIterator<String, Long> results = store.all();
        while (results.hasNext()) {
          final KeyValue<String, Long> next = results.next();
          keyValueBeans.add(new KeyValueBean(next.key, next.value));
        }

      } else {
        String url = "http://" + comppleteHost + "/state/keyval/" + storeName;
        forward(url, keyValueBeans);
      }

    });

    return keyValueBeans;

  }

  private void forward(String url, List<KeyValueBean> results) {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    HttpGet httpGet = new HttpGet(url);
    try {
      CloseableHttpResponse response = httpclient.execute(httpGet);
      String jsonInput = EntityUtils.toString(response.getEntity());
      ObjectMapper mapper = new ObjectMapper();
      List<KeyValueBean> responses = mapper.readValue(jsonInput, mapper.getTypeFactory().constructCollectionType(List.class, KeyValueBean.class));
      results.addAll(responses);

    } catch (IOException e) {
      e.printStackTrace();
    }
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
  public void stop() throws Exception {
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
