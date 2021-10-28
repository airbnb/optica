require 'zk'
require 'oj'
require 'hash_deep_merge'
require 'hammerspace'
require 'time'

class StoreCacheWorker

  def initialize(opts)
    @log = opts['log']

    @zk = nil
    connect_to_zk(true)
    setup_watchers
    @cache = Cache.new(opts)
  end

  def start(fetch_interval)
    connect_to_zk(true)
    # zk watcher for node joins/leaves
    @cache_root_watcher = nil

    reload_instances
    run(fetch_interval)
  end

  def stop
    @log.warn "stopping cache worker"
    @store.stop
    @cache_root_watcher.unsubscribe if @cache_root_watcher
    @cache_root_watcher = nil
    @cache.close
  end

  def load_instances_from_zk
    @log.info "Reading instances from zk:"
    from_server = {}

    begin
      @zk.children('/', :watch => true).each do |child|
        from_server[child] = get_node("/#{child}")
      end
    rescue Exception => e
      # ZK client library caches DNS names of ZK nodes and it resets the
      # cache only when the client object is initialized, or set_servers
      # method is called. Set_servers is not exposed in ruby library, so
      # we force re-init the underlying client object here to make sure
      # we always connect to the current IP addresses.
      @zk.reopen

      @log.error "unexpected error reading from zk! #{e.inspect}"
      raise e
    end

    from_server
  end

  # immediately update cache if node joins/leaves
  def setup_watchers
    return if @zk.nil?

    @cache_root_watcher = @zk.register("/", :only => :child) do |event|
      @log.info "Children added/deleted"
      reload_instances
    end
  end

  def reload_instances()
    instances = load_instances_from_zk.freeze
    @cache.set_results(instances)
  end

  def run(fetch_interval)
    while true do
      begin
        sleep(fetch_interval) rescue nil
        @log.info "Cache fetch thread now fetches from zk..."
        reload_instances rescue nil
        throw_if_stale
      rescue => ex
        @log.warn "Caught exception in cache fetch thread: #{ex} #{ex.backtrace}"
      end
    end
  end
end
