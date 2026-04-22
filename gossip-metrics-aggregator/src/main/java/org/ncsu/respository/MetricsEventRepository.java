package org.ncsu.respository;

import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.ncsu.entity.MetricsEvent;

@ApplicationScoped
public class MetricsEventRepository implements PanacheRepository<MetricsEvent> {

    @Transactional
    public boolean save(MetricsEvent metricsEvent) {
        persist(metricsEvent);
        return true;
    }

}
