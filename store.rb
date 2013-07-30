require 'zk'
require 'fog'
require 'json'
require 'logger'
require 'hash_deep_merge'

class Store

  attr_reader :ips

  def initialize(opts)
    @log = Logger.new(STDOUT)
    @log.progname = self.class.name
    @log.level = Logger::INFO unless opts['debug']

    %w{zk_path aws_access_key aws_secret_key}.each do |req|
      raise ArgumentError, "missing required argument '#{req}'" unless opts[req]
    end

    @path = opts['zk_path']
    @creds = {:provider => 'AWS', :aws_access_key_id => opts['aws_access_key'], :aws_secret_access_key => opts['aws_secret_key']}

    @sync_interval = 30
    @sync_interval = opts['sync_interval'].to_i if opts.include?('sync_interval')

    @zk = nil
    @ips = []
    @last_sync = Time.new(0)
  end

  def start()
    @log.info "waiting to connect to zookeeper at #{@path}"
    @zk = ZK.new(@path)
    @zk.ping?

    @log.info 'ZK connection established successfully'
    @sync_thread = Thread.new{sync}
  end

  def stop()
    Process.kill("TERM", Process.pid) unless $EXIT

    @log.debug 'stopping sync thread'
    @sync_thread.kill
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
    end

    from_server
  end

  def add(node, data)
    child = "/#{node}"

    # deep-merge the old and new data
    prev_data = get_node(child)
    data = prev_data.deep_merge(data).to_json

    @log.debug "writing to zk at #{child} with #{data}"

    begin
      @zk.set(child, data)
    rescue ZK::Exceptions::NoNode => e
      @zk.create(child, :data =>data)
    rescue Exception => e
      @log.error "unexpected error writing to zk! #{e.inspect}"
      stop
    end
  end

  def delete(node)
    @log.info "deleting node #{node}"

    begin
      @zk.delete("/" + node, :ignore => :no_node)
    rescue Exception => e
      @log.error "unexpected error deleting nodes in zk! #{e.inspect}"
      stop
    end
  end

  def healthy?()
    healthy = true
    if $EXIT
      @log.warn 'not healthy because stopping...'
      healthy = false
    elsif (Time.now() - @last_sync) > (4 * @sync_interval)
      @log.warn 'not healthy because too long since sync...'
      healthy = false
    elsif not @zk
      @log.warn 'not healthy because no zookeeper...'
      healthy = false
    elsif not @zk.ping?
      @log.warn 'not healthy because zookeeper not available...'
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
    end
  end

  def sync()
    @log.debug 'starting sync thread'

    while not $EXIT
      @log.debug 'starting sync'
      Timeout.timeout(4 * @sync_interval) { sync_aws }

      @last_sync = Time.now

      unless $EXIT
        @log.info "sync complete, sleeping for #{@sync_interval}"
        sleep @sync_interval
      end
    end

  rescue Exception => e
    @log.error "unexpected exception in store sync thread! #{e.inspect}"
    stop
  end

  def sync_aws()
    @log.debug 'list all ips on all instances in ec2'
    ips = ['127.0.0.1']  # always included for dev purposes

    f = Fog::Compute.new(@creds)
    f.describe_regions.body['regionInfo'].each do |regionInfo|
      con = Fog::Compute.new(
        @creds.merge(:region => regionInfo['regionName']))
      con.servers.each do |server|
        ips << server.private_ip_address
      end

      @log.debug "#{ips.count} ips so far..."
    end

    # save aws ips
    @ips = ips

    cur_nodes = nodes

    stale = cur_nodes.keys.select{ |ip| not ips.include? ip }
    ratio = (stale.count.to_f / cur_nodes.count.to_f) * 100

    if ratio > 10
      @log.warn "#{stale.count} of #{cur_nodes.count} stale nodes is too many; skipping cleanup"
    else
      @log.info "Cleaning up #{stale.count} stale nodes (#{ratio}%)"
      stale.each do |ip|
        @log.info "deleting stale node #{ip} (#{cur_nodes[ip].inspect})"
        delete(ip)
      end
    end
  end
end
