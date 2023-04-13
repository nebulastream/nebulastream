# The base image is the NES executable image to run the coordinator and a worker.
FROM nebulastream/nes-executable-image:latest AS execimage

# Create a file for the demo resources and the output
RUN mkdir -p /resources /output

# The CSV query writes to the /tutorial folder
RUN ln -s /output /tutorial

# Install curl to launch the query, vim and tmux for debugging
RUN apt-get update -q && DEBIAN_FRONTEND="noninteractive" apt-get install --no-install-recommends -q -y vim-tiny tmux curl

# Copy the entry point shell script.
COPY ./entrypoint.sh /
COPY conf-old/coordinator.yml /resources
COPY conf-old/worker-1.yml /resources
COPY input-data-old/wind-turbine-1.csv /resources
COPY rest-client/rest-query-with-csv-sink.json /resources

ENTRYPOINT ["/entrypoint.sh"]
