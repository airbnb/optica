
require 'amqp'
require 'thread'

class Events
  def initialize(opts)
    @log = Logger.new(STDOUT)
    @log.progname = self.class.name
    @log.level = Logger::INFO unless opts['debug']

    %w{rabbit_host rabbit_port}.each do |req|
      raise ArgumentError, "missing required argument '#{req}'" unless opts[req]
    end

    @host = opts['rabbit_host']
    @port = opts['rabbit_port']
    @user = opts['rabbit_user'] || 'guest'
    @pass = opts['rabbit_pass'] || 'guest'

    @exchange_name  = opts['exchange_name']  || 'ops'
    @routing        = opts['routing']        || 'events.node.converged'
    @health_routing = opts['health_routing'] || 'checks.optica'
  end

  def start
    EventMachine.next_tick {
      @connection = AMQP.connect(
        :host => @host,
        :port => @port,
        :user => @user,
        :pass => @pass,
        :heartbeat => 5)

      channel  = AMQP::Channel.new(@connection)
      @exchange = channel.topic(@exchange_name, :durable => true)
    }
  end

  def send(data)
    @exchange.publish(data.to_json, :routing_key => @routing, :persistent => true)
  rescue Exception => e
    @log.error "unexpected error publishing to rabbitmq: #{e.inspect}"
    stop
  else
    @log.debug "published an event to #{@routing}"
  end

  def healthy?
    @exchange.publish("", :routing_key => @health_routing)
  rescue
    false
  else
    @log.debug "events interface healthy"
    true
  end

  def stop
    Process.kill("TERM", Process.pid) unless $EXIT
    @connection.close if @connection
  end
end
