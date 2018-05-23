require 'aws-sdk-sqs'
require 'oj'

class EventsSQS
  def initialize(opts)
    @log = opts['log']

    %w{sqs_region sqs_queue}.each do |req|
      raise ArgumentError, "missing required argument '#{req}'" unless opts[req]
    end

    @opts = {
      :region => opts['sqs_region'],
      :queue => opts['sqs_queue'],
      :logger => @log,
    }

    @message_group_id = opts['routing'] || 'events.node.converged'
    @health_message_group_id = opts['health_routing'] || 'checks.optica'
  end

  def name
    'sqs'
  end

  def start
    @sqs = Aws::SQS::Client.new(region: @opts[:region], logger: @opts[:logger])
    resp = @sqs.get_queue_url(queue_name: @opts[:queue])
    @opts[:queue_url] = resp.queue_url
  end

  def send(data)
    @sqs.send_message(queue_url: @opts[:queue_url],
      message_group_id: @message_group_id, message_body: Oj.dump(data))
    @log.debug "published an event to #{@opts[:queue]}"
  rescue StandardError => e
    @log.error "unexpected error publishing to SQS: #{e.inspect}"
    raise e
  end

  def healthy?
    @sqs.send_message(queue_url: @opts[:queue_url],
      message_group_id: @health_message_group_id, message_body: '{}')
    @log.debug "events interface for SQS is healthy"
    true
  rescue StandardError => e
    @log.error "events interface for SQS failed health check: #{e.inspect}"
    false
  end

  def stop
  end
end
