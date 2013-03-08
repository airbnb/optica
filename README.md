# optica #

Optica is a service for registering and locating nodes.
It provides a simple REST API.

Nodes can POST to / to register themselves with some parameters.
Humans can GET / to get a list of all registered nodes.
GET also accepts some parameters to limit which of the registered nodes you see.

## Installation ##

Use `bundler`!
To install all the dependencies:

```bash
$ bundle install
```

## Testing ##

You'll need a copy of zookeeper running locally, and it should have the right path for optica:

```bash
$ zkServer start
$ zkCli
[zk: localhost:2181(CONNECTED) 0] create /optica ''
Created /optica
[zk: localhost:2181(CONNECTED) 1] quit
Quitting...
```

The example config is set up to talk to your local zookeeper:

```bash
$ cd optica
$ ln -s config.json.example config.json
```

We run `optica` via thin.
To spin up a test process on port 4567:

```bash
$ thin start -p 4567
```

## Data stores ##

We use zookeeper as the backing store for optica.
If you want to use something else, just implement a different `store.rb` class.

## Deployment ##

