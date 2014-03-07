
require 'stomp'

class Events
  def initialize(opts)
    @log = opts['log']

    %w{rabbit_host rabbit_port}.each do |req|
      raise ArgumentError, "missing required argument '#{req}'" unless opts[req]
    end

    @connect_hash = {
      :hosts => [{
          :host => opts['rabbit_host'],
          :port => opts['rabbit_port'],
          :login => opts['rabbit_user'] || 'guest',
          :passcode => opts['rabbit_pass'] || 'guest',
        }],
      :reliable => true,
      :autoflush => true,
      :connect_timeout => 10,
      :logger => @log,
    }

    @exchange_name  = opts['exchange_name']  || 'ops'
    @routing        = opts['routing']        || 'events.node.converged'
    @health_routing = opts['health_routing'] || 'checks.optica'
  end

  def start
    @client = Stomp::Client.new(@connect_hash)
  end

  def send(data)
    @client.publish("/exchange/#{@exchange_name}/#{@routing}", data.to_json, {:persistent => true})
  rescue Exception => e
    @log.error "unexpected error publishing to rabbitmq: #{e.inspect}"
    stop
    raise e
  else
    @log.debug "published an event to #{@routing}"
  end

  def healthy?
    @client.publish("/exchange/#{@exchange_name}/#{@health_routing}", '')
  rescue StandardError => e
    @log.error "events interface failed health check: #{e.inspect}"
    false
  else
    @log.debug "events interface healthy"
    true
  end

  def stop
    @log.warn "stopping the events interface"
    Process.kill("TERM", Process.pid) unless $EXIT
    @client.close if @client
  end
end
