require 'zk'
require 'hash_deep_merge'

class ZkHelper
  def initialize(opts, cache_worker)
    @log = opts['log']

    # zk watcher for node joins/leaves
    @cache_root_watcher = nil

    if opts['zk_path']
      @path = opts['zk_path']
    else
      raise ArgumentError, "missing required argument 'zk_path'"
    end

    @zk = ZK.new(@path)
    connect_to_zk(cache_worker)
  end

  def connect_to_zk(cache_worker)
    @log.info "waiting to connect to zookeeper at #{@path}"

    @zk.on_state_change do |event|
      @log.info "zk state changed, state=#{@zk.state}, session_id=#{session_id}"
    end

    @zk.ping?
    @log.info "ZK connection established successfully. session_id=#{session_id}"

    # We have to read all watchers and refresh cache if we reconnect to a new server.
    @zk.on_connected do |event|
      @log.info "ZK connection re-established. session_id=#{session_id}"

      if !cache_worker.nil?
        @log.info "Resetting watchers and re-syncing cache. session_id=#{session_id}"
        setup_watchers(cache_worker)
        cache_worker.reload_instances
      end
    end
  end

  def get_node(node)
    data, _stat = STATSD.time('optica.zookeeper.get') do
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
    rescue ZK::Exceptions::NoNode
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
    elsif !@zk
      @log.warn 'not healthy because no zookeeper...'
      healthy = false
    elsif !@zk.connected?
      @log.warn 'not healthy because zookeeper not connected...'
      healthy = false
    end
    healthy
  end

  def session_id
    '0x%x' % @zk.session_id
  rescue
    nil
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
  def setup_watchers(cache_worker)
    return if @zk.nil?

    @cache_root_watcher = @zk.register("/", :only => :child) do |event|
      @log.info "Children added/deleted"
      cache_worker.reload_instances
    end
  end

  def close
    @cache_root_watcher.unsubscribe if @cache_root_watcher
    @cache_root_watcher = nil
  end
end
