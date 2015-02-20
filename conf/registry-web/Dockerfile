FROM atcol/docker-registry-ui:latest

# Create a Java trust store from the public key
COPY ca.pem /tmp/ca.pem
RUN keytool -importcert -file /tmp/ca.pem -alias ca -keystore /tmp/truststore -storepass password -noprompt

# Set grails to use the trust store
ENV JAVA_OPTS JAVA_OPTS=-Djavax.net.ssl.trustStore=/tmp/truststore -Djavax.net.ssl.trustStorePassword=password

# Add the repository
ENV REG1 https://kat:kat@sdp-ingest5.kat.ac.za:5000/v1/

CMD ["/usr/share/tomcat7/bin/custom-startup.sh"]
