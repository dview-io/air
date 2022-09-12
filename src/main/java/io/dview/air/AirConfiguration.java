package io.dview.air;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * This {@code AirConfiguration} is responsible
 * to configure pinot connection details
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AirConfiguration {
    private String endPoint;
    private String authToken;
    private int poolSize = 5;
    private String query;
    private long timeoutMs = 10000;
    private boolean enablePool = false;
    private int defaultConnTimeout = 5000;
}
