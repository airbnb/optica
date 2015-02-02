require 'zk'
require 'json'
require 'hash_deep_merge'

class Store

  attr_reader :ips

  def initialize(opts)
    @log = opts['log']

    unless opts['zk_path']
      raise ArgumentError, "missing required argument 'zk_path'"
    else
      @path = opts['zk_path']
    end

    @zk = nil
    setup_cache(opts)
  end

  def setup_cache(opts)
    # We use a daemon that refreshes cache every N (tunable)
    # seconds. In addition, we subscript to all children joining/leaving
    # events. This is less frequent because normally no one would constantly
    # add/remove machines. So whenever a join/leave event happens, we immediately
    # refresh cache. This way we guarantee that whenever we add/remove
    # machines, cache will always have the right set of machines.

    @cache_enabled = !!opts['cache_enabled']

    # zk watcher for node joins/leaves
    @cache_root_watcher = nil

    # mutex for atomically updating cached results
    @cache_mutex = Mutex.new
    @cache_results = {}

    # daemon that'll fetch from zk periodically
    @cache_fetch_thread = nil
    # flag that controls if fetch daemon should run
    @cache_fetch_thread_should_run = false
    # how long we serve cached data
    @cache_fetch_interval = (opts['cache_fetch_interval'] || 20).to_i

    # timestamp that prevents setting cache result with stale data
    @cache_results_last_fetched_time = Time.now
  end

  def start()
    @log.info "waiting to connect to zookeeper at #{@path}"
    @zk = ZK.new(@path)
    @zk.on_state_change do |event|
      @log.info "zk state changed, state=#{@zk.state}, session_id=#{session_id}"
    end
    @zk.ping?
    @log.info "ZK connection established successfully. session_id=#{session_id}"

    if @cache_enabled then
      watch
      reload_instances
      start_fetch_thread
    end
  end

  def session_id
    '0x%x' % @zk.session_id rescue nil
  end

  def stop_cache_related()
    @cache_root_watcher.unsubscribe if @cache_root_watcher
    @cache_root_watcher = nil
    @cache_fetch_thread_should_run = false
    @cache_fetch_thread.join if @cache_fetch_thread
    @cache_fetch_thread = nil
  end

  def stop()
    @log.warn "stopping the store"
    stop_cache_related
    @zk.close() if @zk
    @zk = nil
  end

  # get instances for a given service
  def nodes()
    STATSD.time('optica.store.get_nodes') do
      return load_instances_from_zk unless @cache_enabled
      @cache_results.clone
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
    json_data = new_data.to_json

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
        JSON.parse(data)
      end
    rescue ZK::Exceptions::NoNode
      @log.info "node #{node} disappeared"
      {}
    rescue JSON::ParserError
      @log.warn "removing invalid node #{node}: data failed to parse (#{data.inspect})"
      delete(node)
      {}
    rescue Exception => e
      @log.error "unexpected error reading from zk! #{e.inspect}"
      raise e
    end
  end

  # immediately update cache if node joins/leaves
  def watch()
    return if @zk.nil?
    @cache_root_watcher ||= @zk.register("/", :only => :child) do |event|
      @log.info "Children added/deleted"
      reload_instances
    end
  end

  def reload_instances()
    # Here we use local time to preven race condition
    # Basically cache fetch thread or zookeeper watch callback
    # both will call this to refresh cache. Depending on which
    # finishes first our cache will get set by the slower one.
    # So in order to prevent setting cache to an older result,
    # we set both cache and the timestamp of that version fetched
    # Since timestamp will be monotonically increasing, we are
    # sure that cache set will always have newer versions

    fetch_start_time = Time.now
    instances = load_instances_from_zk()
    @cache_mutex.synchronize do
      if fetch_start_time > @cache_results_last_fetched_time then
        @cache_results_last_fetched_time = fetch_start_time
        @cache_results = instances
      end
    end
  end

  def start_fetch_thread()
    @cache_fetch_thread_should_run = true
    @cache_fetch_thread = Thread.new do
      while @cache_fetch_thread_should_run do
        begin
          sleep(@cache_fetch_interval) rescue nil
          @log.info "Cache fetch thread now fetches from zk..."
          reload_instances rescue nil
        rescue => ex
          @log.warn "Caught exception in cache fetch thread: #{ex} #{ex.backtrace}"
        end
      end
    end
  end
end
