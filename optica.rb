require 'sinatra/base'
require 'cgi'
require 'oj'

class Optica < Sinatra::Base
  configure :production, :development do
    enable :logging
  end

  get '/' do
    return get_nodes(request)
  end

  # endpoint for fab usage
  get '/roles' do
    fields_to_include = ['role', 'id', 'hostname']
    params = CGI::parse(request.query_string)
    if params['_extra_fields']
      values = params['_extra_fields']
      # accept both _additional_fields[] and _additional_fields=1,2 syntax
      values.each do |value|
        fields_to_include += value.split(',')
      end
    end

    return get_nodes(request, fields_to_include)
  end

  def get_nodes(request, fields_to_include=nil)
    params = CGI::parse(request.query_string).reject { |p| p[0] == '_' }

    # include only those nodes that match passed-in parameters
    examined = 0
    to_return = {}
    begin
      nodes = settings.store.nodes
    rescue
      halt(503)
    end

    nodes.each do |node, properties|
      examined += 1
      included = true

      params.each do |param, values|
        values.each do |value|

          if not properties.include? param
            included = false
          elsif properties[param].nil?
            included = false
          elsif properties[param].is_a? String
            included = false unless properties[param].match value
          elsif properties[param].is_a? Array
            included = false unless properties[param].include? value
          elsif properties[param].class == TrueClass
            included = false unless ['true', 'True', '1'].include? value
          elsif properties[param].class == FalseClass
            included = false unless ['false', 'False', '0'].include? value
          end
        end
      end

      if included
        # return full list if fields_to_include is nil. Otherwise return only keys
        # listed in fields_to_include. Not using slice because not a rails app.
        to_return[node] = fields_to_include.nil? ? properties : properties
          .select { |key, _value| fields_to_include.include? key }
      end
    end

    content_type 'application/json', :charset => 'utf-8'
    result = {'examined'=>examined, 'returned'=>to_return.count, 'nodes'=>to_return}
    return Oj.dump(result)
  end

  post '/' do
    begin
      data = Oj.load request.body.read
    rescue JSON::ParserError
      data = {}
    end

    ip = data['ip']

    # check the node ip? disabled by false and nil
    case settings.ip_check
    when :direct
      halt(403) unless ip == request.ip
    when :forwarded_for
      header = env['HTTP_X_FORWARDED_FOR']
      halt(500) unless header
      halt(403) unless ip == header.split(',').first
    end

    # update main store
    begin
      merged_data = settings.store.add(ip, data)
    rescue
      halt(500)
    end

    # publish update event
    message = 'stored'
    begin
      event = merged_data.merge('event' => data)
      settings.events.each do |events|
        events.send(event)
      end
    rescue => e
      # If event publishing failed, we treat it as a warning rather than an error.
      message += " -- [warning] failed to publish event: #{e.to_s}"
    end

    content_type 'text/plain', :charset => 'utf-8'

    return message
  end

  delete '/:id' do |id|
    matching = settings.store.nodes.select{ |k,v| v['id'] == id }
    if matching.length == 0
      return 204
    elsif matching.length == 1
      begin
        settings.store.delete(matching.flatten[1]['ip'])
      rescue
        halt(500)
      else
        return "deleted"
      end
    else
      return [409, "found multiple entries matching id #{id}"]
    end
  end

  get '/health' do
    if settings.store.healthy? and settings.events.all? { |events| events.healthy? }
      content_type 'text/plain', :charset => 'utf-8'
      return "OK"
    else
      return [503, 'not healthy!']
    end
  end

  get '/ping' do
    content_type 'text/plain', :charset => 'utf-8'
    return "PONG"
  end
end
