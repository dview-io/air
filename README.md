Pinot Connection Polling and Resulting as list of Objects.

    - AirConfiguration
        - endPoint : 'jdbc:pinot://localhost:9000' { Connection End Point }
          authToken : If auth is enabled
          poolSize: The Connection pool Size, default is 10
          query: If you want to run this as standalone application.

    - Ussage
        - AirApplication airApplication = new AirApplication(airConfiguration);
          airApplication.submitQuery("select * from airlineStats limit 10");