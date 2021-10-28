require 'zk'
require 'oj'
require 'hash_deep_merge'
require 'hammerspace'
require 'time'

class Store

  attr_reader :ips

  DEFAULT_CACHE_STALE_AGE = 0
  H_DIR = "/tmp/hammerspace".freeze
  H_UPDATE_TIME_KEY = "update_time".freeze
  H_CACHED_RESULTS_KEY = "cached_results".freeze

  def initialize(opts)
    @log = opts['log']

    unless opts['zk_path']
      raise ArgumentError, "missing required argument 'zk_path'"
    else
      @path = opts['zk_path']
    end

    @zk = nil
    @h = nil
    @cache_enabled = !!opts['cache_enabled']
    @cache_worker = nil
    if @cache_enabled
      @cache_worker = StoreCacheWorker.new(opts)
    end
    @cache_stale_age = opts['cache_stale_age'] || DEFAULT_CACHE_STALE_AGE
  end

  def start_reader
    if @cache_enabled
      
    else
      connect_to_zk(false)
    end
  end

  def start_cache_worker(fetch_interval)
    connect_to_zk(true)
    # zk watcher for node joins/leaves
    @cache_root_watcher = nil

    # concurrency safe cache
    @h = Hammerspace.new(H_DIR)
    @h[H_UPDATE_TIME_KEY] = Time.now.to_s

    setup_watchers
    reload_instances
    run_cache_worker(fetch_interval)
  end

  def connect_to_zk(is_cache_worker)
    @log.info "waiting to connect to zookeeper at #{@path}"
    @zk = ZK.new(@path)

    @zk.on_state_change do |event|
      @log.info "zk state changed, state=#{@zk.state}, session_id=#{session_id}"
    end

    @zk.ping?
    @log.info "ZK connection established successfully. session_id=#{session_id}"

    # We have to read all watchers and refresh cache if we reconnect to a new server.
    @zk.on_connected do |event|
      @log.info "ZK connection re-established. session_id=#{session_id}"

      if is_cache_worker
        @log.info "Resetting watchers and re-syncing cache. session_id=#{session_id}"
        setup_watchers
        reload_instances
      end
    end
  end

  def session_id
    '0x%x' % @zk.session_id rescue nil
  end

  def stop_cache_worker
    @log.warn "stopping cache worker"
    stop
    @cache_root_watcher.unsubscribe if @cache_root_watcher
    @cache_root_watcher = nil
    @h.close
  end

  def stop
    @log.warn "stopping the store"
    @zk.close() if @zk
    @zk = nil
  end

  # get instances for a given service
  def nodes()
    STATSD.time('optica.store.get_nodes') do
      return load_instances_from_zk unless @cache_enabled

      check_cache_age
      @cache_worker.get_cached_results
    end
  end

  def load_instances_from_zk()
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

  def add(node, data)
    child = "/#{node}"

    # deep-merge the old and new data
    prev_data = get_node(child)
    new_data = prev_data.deep_merge(data)
    json_data = Oj.dump(new_data)

    @log.debug "writing to zk at #{child} with #{json_data}"

    begin
      STATSD.time('optica.zookeeper.set') do
        @zk.set(child, json_data)
      end
      new_data
    rescue ZK::Exceptions::NoNode => e
      STATSD.time('optica.zookeeper.create') do
        @zk.create(child, :data => json_data)
      end
      new_data
    rescue Exception => e
      @zk.reopen

      @log.error "unexpected error writing to zk! #{e.inspect}"
      raise e
    end
  end

  def delete(node)
    @log.info "deleting node #{node}"

    begin
      STATSD.time('optica.zookeeper.delete') do
        @zk.delete("/" + node, :ignore => :no_node)
      end
    rescue Exception => e
      @zk.reopen

      @log.error "unexpected error deleting nodes in zk! #{e.inspect}"
      raise e
    end
  end

  def healthy?()
    healthy = true
    if $EXIT
      @log.warn 'not healthy because stopping...'
      healthy = false
    elsif not @zk
      @log.warn 'not healthy because no zookeeper...'
      healthy = false
    elsif not @zk.connected?
      @log.warn 'not healthy because zookeeper not connected...'
      healthy = false
    end
    return healthy
  end

  private
  def get_node(node)
    begin
      data, stat = STATSD.time('optica.zookeeper.get') do
        @zk.get(node)
      end
      STATSD.time('optica.json.parse') do
        Oj.load(data)
      end
    rescue ZK::Exceptions::NoNode
      @log.info "node #{node} disappeared"
      {}
    rescue JSON::ParserError
      @log.warn "removing invalid node #{node}: data failed to parse (#{data.inspect})"
      delete(node)
      {}
    rescue Exception => e
      @zk.reopen

      @log.error "unexpected error reading from zk! #{e.inspect}"
      raise e
    end
  end

  # immediately update cache if node joins/leaves
  def setup_watchers
    return if @zk.nil?

    @cache_root_watcher = @zk.register("/", :only => :child) do |event|
      @log.info "Children added/deleted"
      reload_instances
    end
  end

  def check_cache_age
    return unless @cache_enabled
    @cache_worker.throw_if_stale
  end

  def reload_instances()
    instances = load_instances_from_zk.freeze
    set_cached_results(instances)
  end

  def get_cache_age_secs()
    last_fetched_time = Time.parse(@h[H_UPDATE_TIME_KEY])
    Time.new.to_i - last_fetched_time.to_i
  end

  def set_cached_results(results)
    # Hammerspace is threadsafe and latest write "wins"
    # so no need to guard with mutex
    @h[H_UPDATE_TIME_KEY] = Time.now.to_s
    @h[H_CACHED_RESULTS_KEY] = results.to_json
    flush_cache
  end

  def flush_cache()
    @h.close
    @h = Hammerspace.new(H_DIR)
  end
end
