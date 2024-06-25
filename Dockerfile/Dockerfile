# Use an official Eclipse Mosquitto image as the base image
FROM eclipse-mosquitto:latest

# Copy configuration file to the appropriate directory
COPY mosquitto.conf /mosquitto/config/mosquitto.conf

# Copy password file to the appropriate directory
COPY passwd /mosquitto/config/passwd

# Expose the ports that Mosquitto uses
EXPOSE 1883
EXPOSE 9001

# Run Mosquitto broker
CMD ["/usr/sbin/mosquitto", "-c", "/mosquitto/config/mosquitto.conf"]
