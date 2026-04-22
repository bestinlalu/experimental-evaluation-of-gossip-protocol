package org.ncsu.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.ncsu.entity.MetricsEvent;
import org.ncsu.respository.MetricsEventRepository;

@ApplicationScoped
public class MetricsService {

    @Inject
    MetricsEventRepository metricsEventRepository;

    private static final Logger LOG = Logger.getLogger(MetricsService.class);

    public Boolean saveMetricsEvent(MetricsEvent metricsEvent) {

        try {
            return metricsEventRepository.save(metricsEvent);
        } catch (Exception e) {
            LOG.error(e);
            return false;
        }
    }
}
