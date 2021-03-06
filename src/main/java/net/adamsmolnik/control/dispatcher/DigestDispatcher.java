package net.adamsmolnik.control.dispatcher;

import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.StatusType;
import net.adamsmolnik.exceptions.ServiceException;
import net.adamsmolnik.model.digest.DigestRequest;
import net.adamsmolnik.model.digest.DigestResponse;
import net.adamsmolnik.sender.Sender;
import net.adamsmolnik.sender.SendingParams;
import net.adamsmolnik.util.Configuration;
import net.adamsmolnik.util.LocalServiceUrlCache;
import net.adamsmolnik.util.Log;
import net.adamsmolnik.util.Scheduler;
import net.adamsmolnik.util.Util;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

/**
 * @author ASmolnik
 *
 */
@Dependent
public class DigestDispatcher {

    @Inject
    private LocalServiceUrlCache cache;

    @Inject
    private Log log;

    @Inject
    private Configuration conf;

    @Inject
    private Sender<DigestRequest, DigestResponse> sender;

    @Inject
    private Scheduler scheduler;

    private String serviceName;

    private String basicServerDomain;

    private final String serviceContext = "/digest-service-no-limit";

    private final String servicePath = "/ds/digest";

    private final String serviceFullPath = serviceContext + servicePath;

    private final Class<DigestResponse> responseClass = DigestResponse.class;

    private long sizeThreshold;

    private AmazonEC2 ec2;

    @PostConstruct
    private void init() {
        ec2 = new AmazonEC2Client();
        serviceName = conf.getServiceName();
        basicServerDomain = conf.getServiceValue("basicServerDomain");
        sizeThreshold = Long.valueOf(conf.getServiceValue("sizeThreshold"));
    }

    public DigestResponse execute(DigestRequest digestRequest) {
        /*
         * Complete code here and produce meaningful DigestResponse in lieu of placeholder as below
         * 
         */
        return new DigestResponse();
    }

    private long fetchObjectSize(DigestRequest digestRequest) {
        Client client = ClientBuilder.newClient();
        String fetchSizeUrl;
        try {
            fetchSizeUrl = buildServiceContextUrl(basicServerDomain) + "/ds/objects/"
                    + URLEncoder.encode(digestRequest.objectKey, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException unsupportedEx) {
            throw new ServiceException(unsupportedEx);
        }
        Response sizeResponse = client.target(fetchSizeUrl).queryParam("metadata", "size").request().get();
        StatusType statusType = sizeResponse.getStatusInfo();
        if (statusType.getStatusCode() == Status.OK.getStatusCode()) {
            return Long.valueOf(sizeResponse.readEntity(String.class));
        } else {
            throw new ServiceException("Retrieving the size with url " + fetchSizeUrl + " failed with status " + statusType + " and content "
                    + sizeResponse.readEntity(String.class));
        }
    }

    protected void sendHealthCheckUntilGetsHealthy(String newAppUrl) {
        String healthCheckUrl = newAppUrl + "/hc";
        AtomicInteger hcExceptionCounter = new AtomicInteger();
        scheduler.scheduleAndWaitFor(() -> {
            try {
                URL url = new URL(healthCheckUrl);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setConnectTimeout(2000);
                con.setRequestMethod("GET");
                con.connect();
                int rc = con.getResponseCode();
                log.info("Healthcheck response code of " + rc + " received for " + healthCheckUrl);
                return HttpURLConnection.HTTP_OK == rc ? Optional.of(rc) : Optional.empty();
            } catch (Exception ex) {
                int c = hcExceptionCounter.incrementAndGet();
                log.err("HC attempt (" + c + ") for " + healthCheckUrl + " has failed due to " + ex.getLocalizedMessage());
                log.err(ex);
                if (c > 2) {
                    throw new ServiceException(ex);
                }
                return Optional.empty();
            }
        }, 15, 300, TimeUnit.SECONDS);
    }

    private String buildServiceContextUrl(String serverAddress) {
        return "http://" + serverAddress + serviceContext;
    }

    private String buildServiceUrl(String serverAddress) {
        return "http://" + serverAddress + serviceFullPath;
    }

}
