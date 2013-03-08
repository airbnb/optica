require 'zk'
require 'json'

class Store
  attr_reader :nodes

  def initialize(opts)
    unless opts['zk_path']
      raise ArgumentError, "you need to specify required argument 'zk_path'"
    else
      @path = opts['zk_path']
    end

    @nodes = {}
  end

  def start()
    puts "waiting to connect to zookeeper at #{@path}"
    @zk = ZK.new(@path)
    refresh
  end

  def add(node, data)
    @nodes[node] = data

    puts "writing to zk at #{node} with #{data.inspect}"

    node.insert(0, '/')
    data = data.to_json
    begin
      @zk.set(node, data)
    rescue ZK::Exceptions::NoNode => e
      @zk.create(node, :data =>data)
    end
  end

  def delete(node)
    puts "deleting node #{node}"
    @nodes.delete(node)

    @zk.delete(node.insert(0,'/'), :ignore => :no_node)
  end

  def refresh()
    new_nodes = {}

    @zk.children('/').each do |child|
      begin
        data, stat = @zk.get("/#{child}")
        new_nodes[child] = JSON.parse(data)
      rescue ZK::Exceptions::NoNode
        next
      rescue JSON::ParserError
        puts "removing invalid node #{child}: data failed to parse (#{child_info.inspect})"
        delete(child)
      end
    end

    @nodes = new_nodes
  end

end
