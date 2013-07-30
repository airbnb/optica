require 'json'
opts = JSON.parse( File.read('config.json') )

# prepare to exit cleanly
$EXIT = false

# configure the store
require './store.rb'
store = Store.new(opts)
store.start

# configure the event creator
require './events.rb'
events = Events.new(opts)
events.start

# set a signal handler
['INT', 'TERM', 'QUIT'].each do |signal|
  trap(signal) do
    exit! if $EXIT
    $EXIT = true

    # stop the server
    server = Rack::Handler.get(server) || Rack::Handler.default
    server.shutdown if server.respond_to?(:shutdown)

    # stop the components
    store.stop()
    events.stop()
    exit!
  end
end

# start the app
require './optica.rb'
Optica.set :store, store
Optica.set :events, events
run Optica
