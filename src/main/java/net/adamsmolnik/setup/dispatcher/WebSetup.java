package net.adamsmolnik.setup.dispatcher;

import java.util.Map;
import javax.inject.Inject;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import net.adamsmolnik.control.dispatcher.DigestDispatcher;
import net.adamsmolnik.endpoint.QueueEndpoint;
import net.adamsmolnik.model.digest.DigestRequest;
import net.adamsmolnik.util.Configuration;
import net.adamsmolnik.util.Scheduler;

/**
 * @author ASmolnik
 *
 */
@WebListener("dispatcherSetup")
public class WebSetup implements ServletContextListener {

    @Inject
    private Configuration conf;

    @Inject
    private QueueEndpoint queueEndpoint;

    @Inject
    private DigestDispatcher dc;

    @Inject
    private Scheduler scheduler;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        Map<String, String> confMap = conf.getServiceConfMap();
        queueEndpoint.handleJson(request -> dc.execute(request), DigestRequest.class, confMap.get("queueIn"), confMap.get("queueOut"));
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        queueEndpoint.shutdown();
        scheduler.shutdown();
    }

}
