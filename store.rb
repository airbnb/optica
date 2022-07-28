require 'zk'
require 'oj'
require 'hash_deep_merge'
require 'open-uri'

class Store

  attr_reader :ips

  DEFAULT_CACHE_STALE_AGE  = 0
  DEFAULT_SPLIT_MODE       = "disabled"
  DEFAULT_STORE_PORT       = 8001
  DEFAULT_HTTP_TIMEOUT     = 30
  DEFAULT_HTTP_RETRY_DELAY = 5

  def initialize(opts)
    @log = opts['log']
    @index_fields = opts['index_fields'].to_s.split(/,\s*/)

    @opts = {
      'split_mode'              => DEFAULT_SPLIT_MODE,
      'split_mode_store_port'   => DEFAULT_STORE_PORT,
      'split_mode_retry_delay'  => DEFAULT_HTTP_RETRY_DELAY,
      'split_mode_http_timeout' => DEFAULT_HTTP_TIMEOUT,
    }.merge(opts)

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
    @cache_stale_age = opts['cache_stale_age'] || DEFAULT_CACHE_STALE_AGE

    # zk watcher for node joins/leaves
    @cache_root_watcher = nil

    # mutex for atomically updating cached results
    @cache_results_serialized = nil
    @cache_results = {}
    @cache_indices = {}
    @cache_mutex = Mutex.new

    # daemon that'll fetch from zk periodically
    @cache_fetch_thread = nil
    # flag that controls if fetch daemon should run
    @cache_fetch_thread_should_run = false
    # how long we serve cached data
    @cache_fetch_base_interval = (opts['cache_fetch_interval'] || 20).to_i
    @cache_fetch_interval = @cache_fetch_base_interval

    # timestamp that prevents setting cache result with stale data
    @cache_results_last_fetched_time = 0
  end

  def start()
    @log.info "waiting to connect to zookeeper at #{@path}"
    @zk = ZK.new(@path)

    @zk.on_state_change do |event|
      @log.info "zk state changed, state=#{@zk.state}, session_id=#{session_id}"
    end

    @zk.ping?
    @log.info "ZK connection established successfully. session_id=#{session_id}"

    # We have to readd all watchers and refresh cache if we reconnect to a new server.
    @zk.on_connected do |event|
      @log.info "ZK connection re-established. session_id=#{session_id}"

      if @cache_enabled
        @log.info "Resetting watchers and re-syncing cache. session_id=#{session_id}"
        setup_watchers
        reload_instances
      end
    end

    if @cache_enabled
      setup_watchers
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
      unless @cache_enabled
        inst, idx = load_instances_from_zk
        return inst
      end

      check_cache_age
      @cache_results
    end
  end

  def nodes_serialized
    @cache_results_serialized
  end

  def lookup(params)
    if @opts['split_mode'] != 'server' || !@cache_enabled
      return nodes
    end

    STATSD.time('optica.store.lookup') do

      # Find all suitable indices and their cardinalities
      cardinalities = params.reduce({}) do |res, (key, _)|
        res[key] = @cache_indices[key].length if @cache_indices.key? key
        res
      end

      unless cardinalities.empty?
        # Find best suitable index
        best_key = cardinalities.sort_by {|k,v| v}.first.first
        best_idx = @cache_indices.fetch(best_key, {})

        # Check if index saves enough cycles, otherwise fall back to full cache
        if @cache_results.length > 0 && best_idx.length.to_f / @cache_results.length.to_f > 0.5
          return nodes
        end

        return nodes_from_index(best_idx, params[best_key])
      end

      return nodes
    end
  end

  def load_instances
    STATSD.time('optica.store.load_instances') do
      @opts['split_mode'] == 'server' ?
        load_instances_from_leader :
        load_instances_from_zk
    end
  end

  def load_instances_from_leader
    begin
      uri = "http://localhost:%d/store" % @opts['split_mode_store_port']
      res = open(uri, :read_timeout => @opts['split_mode_http_timeout'])

      remote_store = Oj.safe_load(res.read)
      [ remote_store['inst'], remote_store['idx'] ]
    rescue OpenURI::HTTPError, Errno::ECONNREFUSED, Net::ReadTimeout => e
      @log.error "Error loading store from #{uri}: #{e.inspect}; will retry after #{@opts['split_mode_retry_delay']}"

      sleep @opts['split_mode_retry_delay']
      retry
    end
  end

  def load_instances_from_zk()
    @log.info "Reading instances from zk:"

    inst = {}
    idx = {}

    begin
      @zk.children('/', :watch => true).each do |child|
        node = get_node("/#{child}")
        update_nodes child, node, inst, idx
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

    [inst, idx]
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

  def nodes_from_index(idx, values)
    matching_keys = []

    # To preserve original optica behavior we have to validate all keys
    # against standard rules
    values.each do |val|
      keys = idx.keys.select do |key|
        matched = true
        if key.is_a? String
          matched = false unless key.match val
        elsif key.is_a? Array
          matched = false unless key.include? val
        elsif key.class == TrueClass
          matched = false unless ['true', 'True', '1'].include? val
        elsif key.class == FalseClass
          matched = false unless ['false', 'False', '0'].include? val
        end
        matched
      end
      matching_keys << keys
    end

    if matching_keys.length == 1
      matching_keys = matching_keys.first
    elsif matching_keys.length > 1
      matching_keys = matching_keys.inject(:&)
    end

    matching_keys.reduce({}) do |res, key|
      res.merge idx.fetch(key, {})
    end
  end

  def update_nodes(node_name, node, inst, idx)
    inst[node_name] = node

    @index_fields.each do |key|
      if node.key?(key) && !node[key].nil?
        val = node[key]
        idx[key] ||= {}
        idx[key][val] ||= {}
        idx[key][val][node_name] = node
      end
    end
  end

  def get_node(node)
    begin
      data, stat = STATSD.time('optica.zookeeper.get') do
        @zk.get(node)
      end
      STATSD.time('optica.json.parse') do
        Oj.safe_load(data)
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
    return if @zk.nil? || @opts['split_mode'] == 'server'

    @cache_root_watcher = @zk.register("/", :only => :child) do |event|
      @log.info "Children added/deleted"
      reload_instances
    end
  end

  def check_cache_age
    return unless @cache_enabled

    cache_age = Time.new.to_i - @cache_results_last_fetched_time.to_i
    STATSD.gauge 'optica.store.cache.age', cache_age

    if @cache_stale_age > 0 && cache_age > @cache_stale_age
      msg = "cache age exceeds threshold: #{cache_age} > #{@cache_stale_age}"

      @log.error msg
      raise msg
    end
  end

  def reload_instances()

    return unless @cache_mutex.try_lock

    begin
      now = Time.now.to_i

      if now > @cache_results_last_fetched_time + @cache_fetch_interval
        inst, idx = load_instances

        @cache_results = inst.freeze
        @cache_indices = idx.freeze

        case @opts['split_mode']
        when 'store'
          new_store = {
            'inst' => @cache_results,
            'idx'  => @cache_indices,
          }
          @cache_results_serialized = Oj.dump new_store
        when 'server'
          new_store = {
            'examined' => 0,
            'returned' => @cache_results.length,
            'nodes'    => @cache_results,
          }
          @cache_results_serialized = Oj.dump new_store
        end


        @cache_results_last_fetched_time = now
        update_cache_fetch_interval

        @log.info "reloaded cache. new reload interval = #{@cache_fetch_interval}"
       end
    ensure
      @cache_mutex.unlock
    end
  end

  def update_cache_fetch_interval
    @cache_fetch_interval = @cache_fetch_base_interval + rand(0..20)
  end

  def start_fetch_thread()
    @cache_fetch_thread_should_run = true
    @cache_fetch_thread = Thread.new do
      while @cache_fetch_thread_should_run do
        begin
          sleep(@cache_fetch_interval) rescue nil
          source = @opts['split_mode'] == 'server' ? 'remote store' : 'zookeeper'
          @log.info "Cache fetch thread now fetches from #{source}..."
          reload_instances rescue nil
          check_cache_age
        rescue => ex
          @log.warn "Caught exception in cache fetch thread: #{ex} #{ex.backtrace}"
        end
      end
    end
  end
end
