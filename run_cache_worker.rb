# TODO
# separate out the cache worker from store

# configure the store
require './store.rb'
store = Store.new(opts)

# set a signal handler
['INT', 'TERM', 'QUIT'].each do |signal|
  trap(signal) do
    log.warn "Got signal #{signal} -- exit currently #{$EXIT}"

    exit! if $EXIT
    $EXIT = true

    # stop the server
    server = Rack::Handler.get(server) || Rack::Handler.default
    server.shutdown if server.respond_to?(:shutdown)

    # stop the components
    store.stop_cache_worker()
    events.stop()
    exit!
  end
end

fetch_interval = ( fetch_interval || 20).to_i
