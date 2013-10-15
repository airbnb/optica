# Optica #

Optica is a service for registering and locating nodes.
It provides a simple REST API.

Nodes can POST to / to register themselves with some parameters.
Humans can GET / to get a list of all registered nodes.
GET also accepts some parameters to limit which of the registered nodes you see.

## Why Optica? ##

We love the node registration features of [chef server](http://docs.opscode.com/chef_overview_server.html).
However, we run chef-solo here at [Airbnb](www.airbnb.com).
We use optica as alternate node registration system.

## Installation ##

Use `bundler`!
To install all the dependencies:

```bash
$ bundle install
```

## Dependencies ##

### Zookeeper ###

Optica is a front-end to a data store.
At Airbnb, this data store is [Apache Zookeeper](https://zookeeper.apache.org/).

Why Zookeeper?
* we consider optica information critical data, with high uptime requirements
* we already rely critically on Zookeeper to connect our infrastructure; we strive to ensure maximum uptime for this system
* the load patterns of optica (many reads, infrequenty writes) match what zookeeper provides

### Rabbitmq ###

Some parts of our infrastructure are asynchronous; we rely on notification of converges to know, for example, when some kinds of deploys have completed (or failed).
For this reason, Optica generates events in [rabbitmq](http://www.rabbitmq.com/) for every converge.

## Usage with Chef ##

We've included a sample notifier which reports back to optica on every chef converge.
It's in this repo in `reporter.rb`, just make sure to substitute the
correct value for the `optica_server` option. To use it, we added the [chef-handler cookbook](https://github.com/opscode-cookbooks/chef_handler).
Then, we do the following (in our common cookbook, which is applied to every role):

```ruby
directory node.common.notifier_dir

cookbook_file `reporter.rb` do
  path File.join(node.common.notifier_dir, 'reporter.rb')
end

chef_handler 'notifier' do
  action    :enable
  source    File.join(node.common.notifier_dir, 'reporter.rb')
end
```

If you wish to register additional key-value pairs with your node, simply add them to `node.optica.report`:

```ruby
default.optica.report['jvm_version'] = node.java.version
```

## Usage on the command line ##

Optica has a very minimal query syntax, and errs on the side of returning more information than you need.
Really, the only reason for the query parameters is to limit the amount of data transfered over the network.
We can get away with it because all of the complex functionality you might wish for on the command line is provided by [JQ](http://stedolan.github.io/jq/).

### JQ examples ###

Let's define a basic optica script:
```bash
#!/bin/bash

my_optica_host='https://optica.example.com'
curl --silent ${my_optica_host}/?"$1" | jq --compact-output ".nodes[] | $2"
```

With this in your `$PATH` and the right subsitution for your optica endpoint, here are some examples:

##### Getting all hostnames by role: #####

I run this, then pick a random one to ssh into when, e.g., investigating issues.

`$ optica role=myrole .hostname`

##### How many of each role in us-east-1a or 1b? ####

See what the impact will be of an outage in those two zones:

`$ optica az=us-east 'select(.az == "us-east-1a" or .az == "us-east-1b") | .role' | sort | uniq -c | sort -n `

##### Monitor the progress of a chef run on a role ####

Useful if you've just initiated a chef run across a large number of machines, or are waiting for scheduled runs to complete to deploy your change:

`$ optica role=myrole '[.last_start, .failed, .hostname]' | sort`

## Usage with Fabric ##

We've included a sample `fabfile.py` to get you started.
Simply replace `optica.example` with the address to your optica install.

## Cleanup ##

Optica relies on you manually cleaning up expired nodes.
At Airbnb, all of our nodes run in Amazon's EC2.
We have a regularly scheduled task which grabs all recently terminated instances and performs cleanup, including optica cleanup, on those instances.

Cleanup is accomplished by calling `DELETE` on optica.
For instance:

```bash
$ curl -X DELETE http://optica.example.com/i-36428351
```

## Development ##

You'll need a copy of zookeeper running locally, and it should have the correct path for optica:

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
$ cp config.json.example config.json
```

Edit the default config and add your EC2 credentials.

We run `optica` via thin.
To spin up a test process on port 4567:

```bash
$ thin start -p 4567
```
