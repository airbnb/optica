require 'json'
opts = JSON.parse( File.read('config.json') )

# prepare the logger
require 'logger'
log = Logger.new(STDERR)
log.progname = 'optica'
log.level = Logger::INFO unless opts['debug']

opts['log'] = log

# prepare statsd
require 'statsd-ruby'
STATSD = Statsd.new(opts['statsd_host'], opts['statsd_port'])

# prepare to exit cleanly
$EXIT = false

# configure the store
if opts['es_store']
  require './es_store.rb'
  store = ESStore.new(opts)
else
  require './zk_store.rb'
  store = ZKStore.new(opts)
end

store.start

# configure the event creator
require './events.rb'
events = Events.new(opts)
events.start

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
    store.stop()
    events.stop()
    exit!
  end
end

# do we check the client IP?
ip_check = case opts['client_check']
when true, 'direct' then :direct
when 'forwarded_for' then :forwarded_for
when false, nil then false
else raise 'unknown value for ip_check option'
end

# start the app
require './optica.rb'
Optica.set :store, store
Optica.set :events, events
Optica.set :ip_check, ip_check

log.info "Starting sinatra server..."
run Optica
