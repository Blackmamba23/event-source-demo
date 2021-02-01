# `Event Source App Service Example` 
* This is an example service that will demonstrate an event sourced service. This is also the basic example of how I create the micro-service for my side projects, 
* The server offers 3 endpoints to allow users to do basic CRUD as an example:


### This demo requires Kafka and Zookeeper. Run the docker compose file to spin up these dependancies.
  * Run: `docker-compose up -d`

### The config in this example is loaded via a local JSON file and the default `gizmo/config.Config` struct.
