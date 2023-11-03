package io.dview.air;

import lombok.Data;

/**
 * This {@code AirConfiguration} is responsible
 * to configure pinot connection details
 *
 */
@Data
public class AirConfiguration {
    private String endPoint;
    private String authToken;
    private int poolSize = 5;
    private String query;
    private long timeoutMs = 10000;
    private boolean enablePool = false;
    private int defaultConnTimeout = 5000;
}
