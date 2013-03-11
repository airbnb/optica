require 'zk'
require 'aws-sdk'
require 'json'
require 'logger'

class Store
  attr_reader :nodes

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

    @nodes = {}
    @new_nodes = {}
    @stopping = false
  end

  def start()
    @log.info "configuring aws credentials"
    AWS.config({:access_key_id => @aws_key, :secret_access_key => @aws_secret})

    @log.info "waiting to connect to zookeeper at #{@path}"
    @zk = ZK.new(@path)
    @zk.ping?

    @log.info "ZK connection established successfully"
    Thread.new{sync}

    at_exit { stop }
  end

  def stop()
    @stopping = true

    @log.warn "Writing out new nodes before exiting..."
    write_nodes

    @zk.close!
  end

  def add(node, data)
    @nodes[node] = data
    @new_nodes[node] = data
  end

  def delete(node)
    @log.info "deleting node #{node}"
    @nodes.delete(node)

    @zk.delete(node.insert(0,'/'), :ignore => :no_node)
  end

  private
  def sync()
    @log.debug "starting sync thread"

    while not @stopping
      begin
        @log.debug "starting sync"

        @zk.ping?
        write_nodes
        read_nodes
        sync_aws

        @log.info "sync complete, sleeping for #{@sync_interval}"

        sleep @sync_interval

      rescue Exception => e
        @log.error "unexpected exception in store sync thread! #{e.inspect}"
        start
        break
      end
    end

    @log.info "sync thread exited"
  end

  def write_nodes()
    # write all new nodes to zk
    @log.debug "writing new nodes"
    @new_nodes.each do |node, data|
      child = "/#{node}"
      data = data.to_json
      @log.info "writing to zk at #{child} with #{data}"

      begin
        @zk.set(child, data)
      rescue ZK::Exceptions::NoNode => e
        @zk.create(child, :data =>data)
      end

      @new_nodes.delete(node)
    end
  end

  def read_nodes()
    # refresh our nodes list
    @log.debug "reading new nodes"
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
      end
    end

    @nodes = from_server
  end

  def sync_aws()
    @log.debug "list all ips on all instances in ec2"
    ips = []
    ec2 = AWS::EC2.new()
    ec2.regions.each do |region|
      region.instances.each do |instance|
        ips << instance.private_ip_address
      end

      @log.debug "#{ips.count} ips so far..."
    end

    stale = @nodes.keys.select{ |ip| not ips.include? ip }
    ratio = (stale.count.to_f / @nodes.count.to_f) * 100

    if ratio > 10
      @log.warn "#{stale.count} of #{@nodes.count} stale nodes is too many; skipping cleanup"
    else
      @log.info "Cleaning up #{stale.count} stale nodes (#{ratio}%)"
      stale.each do |ip|
        @log.info "deleting stale node #{ip} (#{@nodes[ip].inspect})"
        delete(ip)
      end
    end
  end
end
