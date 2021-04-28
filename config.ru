require 'oj'
opts = Oj.load( File.read('config.json') )

# prepare the logger
require 'logger'
log = Logger.new(STDERR)
log.progname = 'optica'
log.level = Logger::INFO unless opts['debug']

opts['log'] = log

# Enable GC stats
if opts['gc_stats']
  if defined? GC::Profiler && GC::Profiler.respond_to?(:enable)
    GC::Profiler.enable
  elsif GC.respond_to?(:enable_stats)
    GC.enable_stats
  end
end

# Rack options
if opts['rack']
  key_space_limit = opts['rack']['key_space_limit']
  Rack::Utils.key_space_limit = key_space_limit if key_space_limit
end

# prepare statsd
require 'datadog/statsd'
STATSD = Datadog::Statsd.new(opts['statsd_host'], opts['statsd_port'])

begin
  require 'newrelic_rpm'
  require 'newrelic-zookeeper'
rescue LoadError
  log.info "Newrelic not found, skipping..."
end

# prepare to exit cleanly
$EXIT = false

# configure unicorn-worker-killer
if opts['worker_killer']
  require 'unicorn/worker_killer'

  wk_opts = opts['worker_killer']

  if wk_opts['max_requests']
    max_requests = wk_opts['max_requests']
    # Max requests per worker
    use Unicorn::WorkerKiller::MaxRequests, max_requests['min'], max_requests['max']
  end

  if wk_opts['mem_limit']
    mem_limit = wk_opts['mem_limit']
    # Max memory size (RSS) per worker
    use Unicorn::WorkerKiller::Oom, mem_limit['min'], mem_limit['max']
  end
end

# configure the store
require './store.rb'
store = Store.new(opts)
store.start

EVENTS_CLASSES = {
  'rabbitmq' => {
    'class_name' => 'EventsRabbitMQ',
    'file_name' => './events_rmq.rb',
  },
  'sqs' => {
    'class_name' => 'EventsSQS',
    'file_name' => './events_sqs.rb'
  },
}

events_classes = opts['events'] || ['rabbitmq']

# configure the event creator
events = events_classes.map do |name|
  class_opts = EVENTS_CLASSES[name]
  raise "unknown value '#{name}' for events option" unless class_opts
  class_name = class_opts['class_name']
  file_name = class_opts['file_name']
  log.info "loading #{class_name} from #{file_name}"
  require file_name
  class_const = Object.const_get(class_name)
  class_const.new(opts).tap do |obj|
    obj.start
  end
end

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

# load the app
require './optica.rb'

# configure tracing client
def datadog_config(log)
  Datadog.configure do |c|
    service = ENV.fetch('DD_SERVICE', 'optica')
    c.use :sinatra, service_name: service
    # Statsd instance used for sending runtime metrics
    c.runtime_metrics.statsd = STATSD
  end

  # register tracer extension
  Optica.register Datadog::Contrib::Sinatra::Tracer

  # add correlation IDs to logger
  log.formatter = proc do |severity, datetime, progname, msg|
    "[#{datetime}][#{progname}][#{severity}][#{Datadog.tracer.active_correlation}] #{msg}\n"
  end
end

begin
  require 'ddtrace/auto_instrument'
  datadog_config(log)
rescue LoadError
  log.info "Datadog's tracing client not found, skipping..."
end

Optica.set :logger, log
Optica.set :store, store
Optica.set :events, events
Optica.set :ip_check, ip_check

# start the app
log.info "Starting sinatra server..."
run Optica
