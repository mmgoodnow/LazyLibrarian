# Telemetry server for LazyLibrarian

Note: The telemetry server is work in progress and is not yet fully
operational.

## Purpose

The telemetry server is intended to collect low-frequency, anonymous data
about how LazyLibrarian is used. The information will be used to inform
future development and improvement, to make sure we spend time on the parts
that are actually being used - and have information on which parts can be
removed because they are unused.

## Client side setup

LazyLibrarian clients can choose the level of data sent, and can entirely
opt out of doing do. Telemetry settings on the client are stored in the
`[Telemetry]` section of the config.ini file.

At this time, no telemetry data is sent to the server.

## Configure and build

The server is intended to be run in Docker, on either Windows or Linux. To
build it for your server, edit the `docker-compose.yml` file, in particular
the volume mappings.

### Editing docker-compose.yml

For the MySQL server, edit the left hand side of volumes to a directory
that should hold the database files:

```
    volumes:
      - C:/code/data/db:/var/lib/mysql
```

For the webL server, edit the left hand side of volumes to a directory
that will hold the server's log files:

```
    volumes:
      - C:/code/data:/data
```

### Building

After installing Docker, run the following command:

`docker-compose build .`

This reads the docker-compose.yml file, downloads the necessary files, and
configures the docker images.

## Running the server

To run the server, run the following command:

`docker-compose up -d`

This brings up both the MySQL docker image and the web server, running them
in the background.

Please refer to the Docker documentation for more information about how to
troubleshoot and debug Docker if it doesn't work as expected.

## Accessing the server

The server has a very simple web interface that can be accessed on port 9174.
Point your browser to [localhost:9174](localhost:9174), and you will get a
simple welcome message.

You can see the commands available by going to the [help](localhost:9174/help)
page, and you can see the current status on the [status](localhost:9174/status)
page.
