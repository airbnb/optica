require 'zk'
require 'fog'
require 'json'
require 'logger'

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
    @aws_key = opts['aws_access_key']
    @aws_secret = opts['aws_secret_key']

    @sync_interval = 30
    @sync_interval = opts['sync_interval'].to_i if opts.include?('sync_interval')

    @zk = nil
    @ips = []
    @stopping = false
    @synced_once = false
  end

  def start()
    @log.info "waiting to connect to zookeeper at #{@path}"
    @zk = ZK.new(@path)
    @zk.ping?

    @log.info "ZK connection established successfully"
    Thread.new{sync}

    at_exit { stop }
  end

  def stop()
    @stopping = true
    Process.kill("TERM", Process.pid)
  end

  def nodes()
    from_server = {}

    @zk.children('/').each do |child|
      begin
        data, stat = @zk.get("/#{child}")
        from_server[child] = JSON.parse(data)
      rescue ZK::Exceptions::NoNode
        @log.info "child #{child} disappeared"
      rescue JSON::ParserError
        @log.warn "removing invalid node #{child}: data failed to parse (#{child_info.inspect})"
        delete(child)
      rescue Exception => e
        @log.error "unexpected error reading from zk! #{e.inspect}"
        stop
      end
    end

    from_server
  end

  def add(node, data)
    child = "/#{node}"
    data = data.to_json
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

  def ping()
    return False if @stopping
    return False unless @zk
    return False unless @synced_once

    @zk.ping?
  end

  private
  def sync()
    @log.debug "starting sync thread"

    while not @stopping
      begin
        @log.debug "starting sync"

        @zk.ping?
        sync_aws

        @synced_once = true

        @log.info "sync complete, sleeping for #{@sync_interval}"
        sleep @sync_interval

      rescue Exception => e
        @log.error "unexpected exception in store sync thread! #{e.inspect}"
        @stopping = true
        break
      end
    end

    @log.info "sync thread exited; stopping everything"
    stop
  end

  def sync_aws()
    @log.debug "list all ips on all instances in ec2"
    ips = []

    creds = {:provider => 'AWS', :aws_access_key_id => @aws_key, :aws_secret_access_key => @aws_secret}
    f = Fog::Compute.new(creds)
    f.describe_regions.body['regionInfo'].each do |regionInfo|
      creds[:region] = regionInfo['regionName']
      con = Fog::Compute.new(creds)
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
