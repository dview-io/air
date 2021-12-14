package io.dview.air;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import static io.dview.air.Constants.OBJECT_MAPPER;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * This {@code AirApplication} is responsible to configure Pinot
 * connection and gives direct interface to write a query.
 */
@Slf4j
public class AirApplication {

    private final AirQuerySubmitter airQuerySubmitter;

    public AirApplication(AirConfiguration airConfiguration) throws SQLException {
        this.airQuerySubmitter = new AirQuerySubmitter(airConfiguration);
    }

    /**
     * @param args linked with {@link String}
     * @throws IOException  linked with {@link Exception}
     * @throws SQLException linked with {@link Exception}
     */
    public static void main(String[] args) throws IOException, SQLException {
        AirConfiguration airConfiguration = OBJECT_MAPPER.findAndRegisterModules()
                .readValue(new File(args[0]), AirConfiguration.class);
        log.debug("The Application Configuration : {}", airConfiguration);
        AirApplication airApplication = new AirApplication(airConfiguration);
        airApplication.submitQuery(airConfiguration.getQuery());
    }

    /**
     * @param query linked with {@link String}
     * @return List  linked with {@link List}
     * @throws SQLException linked with {@link Exception}
     */
    public List<Map<String, Object>> submitQuery(String query) throws SQLException, JsonProcessingException {
        return this.airQuerySubmitter.executeQuery(query);
    }
}