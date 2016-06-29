require 'elasticsearch'
require 'json'
require 'hash_deep_merge'

class ESStore

  attr_reader :ips

  def initialize(opts)
    @log = opts['log']

    unless opts['es_path']
      raise ArgumentError, "missing required argument 'es_path'"
    else
      @path = opts['es_path']
    end

    @port = opts['es_port']

    @es_index = opts['es_index']

    @es = nil
  end

  def start()
    @log.info "waiting to connect to Elasticsearch at #{@path}"
    @es = Elasticsearch::Client.new url: @es_path, port: @port, log: @log
  end

  def stop()
    @log.warn "stopping the store"
    @es = nil
  end

  # get instances for a given service
  def nodes()
    full_nodes = {}
    result = @es.search index: 'optica', search_type: 'scan', scroll: '1m', size: 100

    while result = @es.scroll(scroll_id: result['_scroll_id'], scroll: '5m') and not result['hits']['hits'].empty? do
      result['hits']['hits'].each do | node | 
        source = node["_source"]
        full_nodes[source['ip']] = source
      end
    end

    full_nodes
  end

  def add(node, data)
    @log.debug "writing to zk at #{node} with #{data}"

    id = data["id"]
    # fall back to ip if id is nil
    id ||= node

    begin
      @es.index  index: @es_index, type: 'host', body: data
    rescue Exception => e
      @log.error "unexpected error writing to ES! #{e.inspect}"
      raise e
    end
  end

  def delete(node)
    @log.info "deleting node #{node}"

    begin
      doc = @es.search index: 'optica', type: 'host', q: "ip:#{node}"
      id = doc["hits"]["hits"].first["_id"]
      @es.delete  index: @es_index, type: 'host', id: id
    rescue Exception => e
      @log.error "unexpected error writing to ES! #{e.inspect}"
      raise e
    end
  end

  def healthy?()
    healthy = true
    if $EXIT
      @log.warn 'not healthy because stopping...'
      healthy = false
    elsif not @es
      @log.warn 'not healthy because no Elasticsearch...'
      healthy = false
    elsif not is_es_healthy?
      @log.warn 'not healthy because zookeeper not connected...'
      healthy = false
    end

    return healthy
  end

  private

  def is_es_healthy
    @es.cluster.health["status"] == 'green'
  end
end
