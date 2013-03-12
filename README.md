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

## Usage with Chef ##

We loved the node registration features of [chef server](http://docs.opscode.com/chef_overview_server.html).
However, we run chef-solo here at [Airbnb](www.airbnb.com).
We use optica as alternate node registration system.

We've included a sample notifier which phones back to optica on every chef converge.
It's in this repo in `reporter.rb`.
To use it, we added the [chef-handler cookbook](https://github.com/opscode-cookbooks/chef_handler).
Then, we did the following:

```ruby
directory node.common.notifier_dir

cookbook_file options[:filename] do
  path File.join(node.common.notifier_dir, 'reporter.rb')
end

chef_handler notifier do
  action    :enable
  source    File.join(node.common.notifier_dir, 'reporter.rb')
end
```

If you wish to register additional key-value pairs with your node, simply add them to `node.optica.report`.

## Usage with Fabric ##

We've included a sample `fabfile.py` to get you started.
Simply replace `optica.example` with the address to your optica install.

## Development ##

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

