require 'oj'
opts = Oj.load(File.read('cache_worker_config.json'))

# prepare the logger
require 'logger'
log = Logger.new(STDERR)
log.progname = 'optica'
log.level = Logger::INFO unless opts['debug']

opts['log'] = log

opts['fetch_interval'] = (opts['fetch_interval'] || 20).to_i

# prepare to exit cleanly
$EXIT = false

cache_worker = CacheWorker.new(opts)

# set a signal handler
['INT', 'TERM', 'QUIT'].each do |signal|
  trap(signal) do
    log.warn "Got signal #{signal} -- exit currently #{$EXIT}"

    exit! if $EXIT
    $EXIT = true

    # stop the components
    cache_worker.stop
    exit!
  end
end

cache_worker.start
