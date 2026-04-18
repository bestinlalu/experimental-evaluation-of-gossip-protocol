package org.ncsu.respository;

import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.JsonObject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.core.Response;
import org.ncsu.entity.ActionRecord;

import java.util.List;

@ApplicationScoped
public class ActionRecordRepository implements PanacheRepository<ActionRecord> {

    @Transactional
    public boolean save(ActionRecord actionRecord) {
        actionRecord.id = null;
        persist(actionRecord);
        return true;
    }

}
