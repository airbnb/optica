require 'elasticsearch'
require 'json'
require 'hash_deep_merge'

class ESStore

  attr_reader :ips

  def initialize(opts)
    @log = opts['log']

    @path = opts['es_path'] || '127.0.0.1'
    @port = opts['es_port'] || 9200
    @index = opts['es_index'] || 'optica'
    @batch_size = opts['es_batch_size'] || 1000

    @es = nil
  end

  def start()
    @log.info "waiting to connect to Elasticsearch at #{@path}"
    @es = Elasticsearch::Client.new url: "#{@path}:#{@port}"
  end

  def stop()
    @log.warn "stopping the store"
    @es = nil
  end

  # get instances for a given service
  def nodes(params=nil)
    full_nodes = {}
    q = []
    query = nil
    search_params = {index: 'optica', search_type: 'scan', scroll: '30s', size: @batch_size}
    if params
      params.each do |param, values|
        values.each do |value|
          q << "#{param}:#{value}"
        end
      end

      query = q.join('+')
      search_params.merge!({default_operator: 'AND', q: query})
    end

    result = @es.search(search_params)

    while result = @es.scroll(scroll_id: result['_scroll_id'], scroll: '30s') and not result['hits']['hits'].empty? do
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
      @es.index  index: @index, type: 'host', body: data
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
      @es.delete  index: @index, type: 'host', id: id
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

  def is_es_healthy?
    @es.cluster.health["status"] == 'green'
  end
end
