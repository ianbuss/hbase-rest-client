import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.auth.AuthPolicy;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExampleRestClient {

  private Client client;
  private RemoteHTable remoteHTable;
  private Configuration conf;

  public ExampleRestClient(String hostport) throws IOException {
    System.setProperty("java.security.auth.login.config", "login.conf");
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

    Cluster cluster = new Cluster();
    cluster.add(hostport);
    this.client = new Client(cluster);
    this.conf = HBaseConfiguration.create();

    HttpClient httpClient = this.client.getHttpClient();

    AuthPolicy.registerAuthScheme("Negotiate", NegotiateScheme.class);
    List<String> schemes = new ArrayList<>();
    schemes.add("Negotiate");

    HttpClientParams params = httpClient.getParams();
    params.setParameter(AuthPolicy.AUTH_SCHEME_PRIORITY, schemes);

    httpClient.getState().setCredentials(new AuthScope(null, -1, null), new Credentials() {});
  }

  public void doScan(String tbl, String start, String stop) throws IOException {
    this.remoteHTable = new RemoteHTable(client, conf, tbl);

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(start));
    if (null != stop) {
      scan.setStopRow(Bytes.toBytes(stop));
    }

    ResultScanner scanner = remoteHTable.getScanner(scan);
    for (Result r : scanner) {
      System.out.printf(r.toString());
      CellScanner cScanner = r.cellScanner();
      while (cScanner.advance()) {
        System.out.println(cScanner.current().toString());
      }
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 6) {
      System.err.printf("Usage: %s <hostport> <tbl> <start> <stop>\n",
        ExampleRestClient.class);
    }

    ExampleRestClient client = new ExampleRestClient(args[0]);
    client.doScan(args[3], args[4], args[5]);
  }

}
