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
  end

  def start()
    @log.info "waiting to connect to zookeeper at #{@path}"
    @zk = ZK.new(@path)
    @zk.ping?

    @log.info 'ZK connection established successfully'
  end

  def stop()
    @log.warn "stopping the store"
    Process.kill("TERM", Process.pid) unless $EXIT
    @zk.close() if @zk
  end

  def nodes()
    from_server = {}

    begin
      @zk.children('/').each do |child|
        from_server[child] = get_node("/#{child}")
      end
    rescue Exception => e
      @log.error "unexpected error reading from zk! #{e.inspect}"
      stop
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
      @zk.set(child, json_data)
      new_data
    rescue ZK::Exceptions::NoNode => e
      @zk.create(child, :data => json_data)
      new_data
    rescue Exception => e
      @log.error "unexpected error writing to zk! #{e.inspect}"
      stop
      raise e
    end
  end

  def delete(node)
    @log.info "deleting node #{node}"

    begin
      @zk.delete("/" + node, :ignore => :no_node)
    rescue Exception => e
      @log.error "unexpected error deleting nodes in zk! #{e.inspect}"
      stop
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
      data, stat = @zk.get(node)
      JSON.parse(data)
    rescue ZK::Exceptions::NoNode
      @log.info "node #{node} disappeared"
      {}
    rescue JSON::ParserError
      @log.warn "removing invalid node #{node}: data failed to parse (#{data.inspect})"
      delete(node)
      {}
    rescue Exception => e
      @log.error "unexpected error reading from zk! #{e.inspect}"
      stop
      raise e
    end
  end
end
